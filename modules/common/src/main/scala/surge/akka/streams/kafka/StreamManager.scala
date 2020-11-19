// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.akka.streams.kafka

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ Actor, ActorRef, ActorSystem, Address, Props, Stash }
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{ Committer, Consumer }
import akka.pattern._
import akka.stream.scaladsl.{ Flow, Keep, RestartSource, Sink }
import akka.util.Timeout
import akka.{ Done, NotUsed }
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import surge.akka.cluster.{ ActorHostAwareness, ActorRegistry, ActorSystemHostAwareness }
import surge.akka.streams.graph.PassThroughFlow
import surge.core.DataPipeline.{ ReplayResult, _ }
import surge.core.{ EventReplaySettings, EventReplayStrategy }
import surge.scala.core.kafka.KafkaTopic
import surge.support.Logging

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

object KafkaStreamManager {
  val serviceIdentifier = "StreamManager"

  def apply[Key, Value](topic: KafkaTopic, consumerSettings: ConsumerSettings[Key, Value],
    replayStrategy: EventReplayStrategy,
    replaySettings: EventReplaySettings,
    business: Flow[(Key, Value), Any, _])(implicit actorSystem: ActorSystem, ec: ExecutionContext): KafkaStreamManager[Key, Value] = {
    val subscription = Subscriptions.topics(topic.name)
    val businessFlow = Flow[ConsumerMessage.CommittableMessage[Key, Value]].map(msg ⇒ msg.record.key() -> msg.record.value())
      .via(business)
      .map(_ ⇒ Done)
    new KafkaStreamManager[Key, Value](subscription, topic.name, consumerSettings, replayStrategy, replaySettings, businessFlow)
  }
}

class KafkaStreamManager[Key, Value](
    subscription: Subscription, topicName: String, consumerSettings: ConsumerSettings[Key, Value],
    replayStrategy: EventReplayStrategy, replaySettings: EventReplaySettings, businessFlow: Flow[ConsumerMessage.CommittableMessage[Key, Value], _, NotUsed])(implicit val actorSystem: ActorSystem)
  extends ActorSystemHostAwareness with Logging {

  private val consumerGroup = consumerSettings.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
  private[streams] val managerActor = actorSystem.actorOf(Props(new KafkaStreamManagerActor(subscription, topicName, consumerSettings, businessFlow)))
  private[streams] val replayCoordinator = actorSystem.actorOf(Props(new ReplayCoordinator(topicName, consumerGroup, replayStrategy)))

  def start(): KafkaStreamManager[Key, Value] = {
    managerActor ! KafkaStreamManagerActor.StartConsuming
    this
  }

  def stop(): KafkaStreamManager[Key, Value] = {
    managerActor ! KafkaStreamManagerActor.StopConsuming
    this
  }

  def replay(): Future[ReplayResult] = {
    implicit val entireProcessTimeout: Timeout = Timeout(replaySettings.entireReplayTimeout)
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    (replayCoordinator ? ReplayCoordinator.StartReplay).map {
      case ReplayCoordinator.ReplayCompleted ⇒
        ReplaySuccessfullyStarted()
      case ReplayCoordinator.ReplayFailed(err) ⇒
        ReplayFailed(err)
    }.recoverWith {
      case err: Throwable ⇒
        log.error(s"An unexpected error happened replaying $consumerGroup, " +
          s"please try again, if the problem persists, reach out the Surge team for support", err)
        replayCoordinator ! ReplayCoordinator.StopReplay
        Future.successful(ReplayFailed(err))
    }
  }
}

object KafkaStreamManagerActor {
  type OnReplayComplete = (String, Long) ⇒ Unit

  sealed trait KafkaStreamManagerActorCommand
  case object StartConsuming extends KafkaStreamManagerActorCommand
  case object StopConsuming extends KafkaStreamManagerActorCommand
  sealed trait KafkaStreamManagerActorEvent
  case class SuccessfullyStopped(address: Address, actor: ActorRef) extends KafkaStreamManagerActorEvent
  case object SuccessfullyStarted extends KafkaStreamManagerActorEvent
}
class KafkaStreamManagerActor[Key, Value](subscription: Subscription, topicName: String, baseConsumerSettings: ConsumerSettings[Key, Value],
    businessFlow: Flow[ConsumerMessage.CommittableMessage[Key, Value], _, NotUsed]) extends Actor
  with ActorHostAwareness with Stash with ActorRegistry with Logging {
  import KafkaStreamManagerActor._
  import context.{ dispatcher, system }

  private val config = ConfigFactory.load()
  private val reuseConsumerId = config.getBoolean("surge.kafka-reuse-consumer-id")
  // Set this uniquely per manager actor so that restarts of the Kafka stream don't cause a rebalance of the consumer group
  private val clientId = s"surge-event-source-managed-consumer-${UUID.randomUUID()}"

  private lazy val consumerSettingsWithHost = baseConsumerSettings
    .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[HostAwareRangeAssignor].getName)
    .withProperty(HostAwarenessConfig.HOST_CONFIG, localHostname)
    .withProperty(HostAwarenessConfig.PORT_CONFIG, localPort.toString)
    .withStopTimeout(Duration.Zero)

  private lazy val consumerSettings = if (reuseConsumerId) {
    consumerSettingsWithHost
      .withClientId(clientId)
      .withGroupInstanceId(clientId)
  } else {
    consumerSettingsWithHost
  }

  private case class InternalState(control: AtomicReference[Consumer.Control], streamCompletion: Future[Done])

  override def receive: Receive = stopped

  private def stopped: Receive = {
    case StartConsuming ⇒ startConsumer()
    case StopConsuming  ⇒ sender() ! SuccessfullyStopped(localAddress, self)
  }

  private def consuming(state: InternalState): Receive = {
    case SuccessfullyStarted ⇒ sender() ! SuccessfullyStarted
    case StopConsuming       ⇒ handleStopConsuming(state)
  }

  private def stopping(state: InternalState): Receive = {
    case msg: SuccessfullyStopped ⇒
      handleSuccessfullyStopped()
      sender() ! msg
    case _ ⇒ stash()
  }

  private def startConsumer(): Unit = {
    log.info("Starting consumer for topic {} with client id {}", Seq(topicName, clientId): _*)
    val committerSettings = CommitterSettings(context.system)
    val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)

    val result = RestartSource
      .onFailuresWithBackoff(
        minBackoff = 1.second,
        maxBackoff = 15.seconds,
        randomFactor = 0.1) { () ⇒
        log.debug("Creating Kafka source for topic {} with client id {}", Seq(topicName, clientId): _*)
        Consumer
          .committableSource(consumerSettings, subscription)
          .mapMaterializedValue(c ⇒ control.set(c))
          .via(PassThroughFlow(businessFlow, Keep.right))
          .map(_.committableOffset)
          .via(Committer.flow(committerSettings))
      }.runWith(Sink.ignore)

    val state = InternalState(control, result)
    context.become(consuming(state))
    result.map(_ ⇒ SuccessfullyStarted).pipeTo(self)(sender())
  }

  private def handleStopConsuming(state: InternalState): Unit = {
    val control = state.control.get()
    val drainingControl = DrainingControl(control -> state.streamCompletion)
    log.info("Stopping consumer with client id {} for topic {}", Seq(clientId, topicName): _*)
    context.become(stopping(state))
    drainingControl.drainAndShutdown().map(_ ⇒ SuccessfullyStopped(localAddress, self)).pipeTo(self)(sender())
  }

  private def handleSuccessfullyStopped(): Unit = {
    log.info("Consumer with client id {} for topic {} successfully stopped", Seq(clientId, topicName): _*)
    context.become(stopped)
    unstashAll()
  }

  override def preStart(): Unit = {
    registerService(KafkaStreamManager.serviceIdentifier, self, List(topicName))
    super.preStart()
  }
}
