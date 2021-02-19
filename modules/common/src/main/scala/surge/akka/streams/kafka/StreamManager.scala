// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.akka.streams.kafka

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ Actor, ActorRef, ActorSystem, Address, Props, Stash }
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{ Committer, Consumer }
import akka.pattern._
import akka.stream.scaladsl.{ Flow, RestartSource, Sink }
import akka.util.Timeout
import akka.{ Done, NotUsed }
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{ Metric, MetricName }
import org.slf4j.LoggerFactory
import surge.akka.cluster.{ ActorHostAwareness, ActorRegistry, ActorSystemHostAwareness }
import surge.core.DataPipeline.{ ReplayResult, _ }
import surge.core.{ EventPlusStreamMeta, EventReplaySettings, EventReplayStrategy }
import surge.scala.core.kafka.{ HeadersHelper, KafkaTopic }
import surge.support.Logging

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.Try

object PartitionAssignorConfig {
  private val config = ConfigFactory.load()
  private val log = LoggerFactory.getLogger(getClass)
  val assignorClassName: String = config.getString("surge.kafka-event-source.consumer.range-assignor").toLowerCase() match {
    case "range"              => classOf[HostAwareRangeAssignor].getName
    case "cooperative-sticky" => classOf[HostAwareCooperativeStickyAssignor].getName
    case other =>
      log.warn("Attempted to use an unknown range assignor named [{}], accepted values are [range, cooperative-sticky].  Defaulting to range.", other)
      classOf[HostAwareRangeAssignor].getName
  }
}

case class KafkaStreamMeta(topic: String, partition: Int, offset: Long, committableOffset: CommittableOffset) {
  override def toString: String = {
    s"topic=$topic, partition=$partition, offset=$offset"
  }
}

object KafkaStreamManager {
  val serviceIdentifier = "StreamManager"

  def apply[Key, Value](topic: KafkaTopic, consumerSettings: ConsumerSettings[Key, Value],
    replayStrategy: EventReplayStrategy,
    replaySettings: EventReplaySettings,
    business: Flow[EventPlusStreamMeta[Key, Value, KafkaStreamMeta], KafkaStreamMeta, NotUsed])(implicit
    actorSystem: ActorSystem,
    ec: ExecutionContext): KafkaStreamManager[Key, Value] = {
    val subscription = Subscriptions.topics(topic.name)
    val businessFlow = Flow[ConsumerMessage.CommittableMessage[Key, Value]].map { msg =>
      val kafkaMeta = KafkaStreamMeta(msg.record.topic(), msg.record.partition(), msg.record.offset(), msg.committableOffset)
      EventPlusStreamMeta(msg.record.key, msg.record.value, kafkaMeta, HeadersHelper.unapplyHeaders(msg.record.headers()))
    }.via(business)
    new KafkaStreamManager[Key, Value](subscription, topic.name, consumerSettings, replayStrategy, replaySettings, businessFlow)
  }
}

class KafkaStreamManager[Key, Value](
    subscription: Subscription, topicName: String, consumerSettings: ConsumerSettings[Key, Value],
    replayStrategy: EventReplayStrategy,
    replaySettings: EventReplaySettings,
    businessFlow: Flow[ConsumerMessage.CommittableMessage[Key, Value], KafkaStreamMeta, NotUsed])(implicit val actorSystem: ActorSystem)
  extends ActorSystemHostAwareness with Logging {

  private val config = ConfigFactory.load()
  private val metricFetchTimeout = config.getDuration("surge.kafka-event-source.kafka-metric-fetch-timeout").toMillis.milliseconds
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

  def getMetrics: Future[Map[MetricName, Metric]] = {
    implicit val timeout: Timeout = Timeout(metricFetchTimeout)
    (managerActor ? KafkaStreamManagerActor.GetMetrics).mapTo[Map[MetricName, Metric]]
  }

  // TODO is this Await too expensive? Akka streams only exposes the Kafka metrics from a future even though the Kafka client interfaces expose it non-async
  def getMetricsSynchronous: Map[MetricName, Metric] = {
    Try(Await.result(getMetrics, metricFetchTimeout)).getOrElse(Map.empty)
  }

  def replay(): Future[ReplayResult] = {
    implicit val entireProcessTimeout: Timeout = Timeout(replaySettings.entireReplayTimeout)
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    (replayCoordinator ? ReplayCoordinator.StartReplay).map {
      case ReplayCoordinator.ReplayCompleted =>
        ReplaySuccessfullyStarted()
      case ReplayCoordinator.ReplayFailed(err) =>
        ReplayFailed(err)
    }.recoverWith {
      case err: Throwable =>
        log.error(s"An unexpected error happened replaying $consumerGroup, " +
          s"please try again, if the problem persists, reach out the Surge team for support", err)
        replayCoordinator ! ReplayCoordinator.StopReplay
        Future.successful(ReplayFailed(err))
    }
  }
}

object KafkaStreamManagerActor {
  type OnReplayComplete = (String, Long) => Unit

  sealed trait KafkaStreamManagerActorCommand
  case object StartConsuming extends KafkaStreamManagerActorCommand
  case object StopConsuming extends KafkaStreamManagerActorCommand
  case object GetMetrics extends KafkaStreamManagerActorCommand
  sealed trait KafkaStreamManagerActorEvent
  case class SuccessfullyStopped(address: Address, actor: ActorRef) extends KafkaStreamManagerActorEvent
  case object SuccessfullyStarted extends KafkaStreamManagerActorEvent
}
class KafkaStreamManagerActor[Key, Value](subscription: Subscription, topicName: String, baseConsumerSettings: ConsumerSettings[Key, Value],
    businessFlow: Flow[ConsumerMessage.CommittableMessage[Key, Value], KafkaStreamMeta, NotUsed]) extends Actor
  with ActorHostAwareness with Stash with ActorRegistry with Logging {
  import KafkaStreamManagerActor._
  import context.{ dispatcher, system }

  private val config = ConfigFactory.load()
  private val reuseConsumerId = config.getBoolean("surge.kafka-reuse-consumer-id")
  // Set this uniquely per manager actor so that restarts of the Kafka stream don't cause a rebalance of the consumer group
  private val clientId = s"surge-event-source-managed-consumer-${UUID.randomUUID()}"

  private val backoffMin = config.getDuration("surge.kafka-event-source.backoff.min").toMillis.millis
  private val backoffMax = config.getDuration("surge.kafka-event-source.backoff.max").toMillis.millis
  private val randomFactor = config.getDouble("surge.kafka-event-source.backoff.random-factor")

  private val committerMaxBatch = config.getLong("surge.kafka-event-source.committer.max-batch")
  private val committerMaxInterval = config.getDuration("surge.kafka-event-source.committer.max-interval")
  private val committerParallelism = config.getInt("surge.kafka-event-source.committer.parallelism")
  private val committerSettings = CommitterSettings(context.system)
    .withMaxBatch(committerMaxBatch)
    .withMaxInterval(committerMaxInterval)
    .withParallelism(committerParallelism)

  private lazy val consumerSettingsWithHost = baseConsumerSettings
    .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, PartitionAssignorConfig.assignorClassName)
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
    case StartConsuming => startConsumer()
    case StopConsuming  => sender() ! SuccessfullyStopped(localAddress, self)
    case GetMetrics     => sender() ! Map.empty
  }

  private def consuming(state: InternalState): Receive = {
    case SuccessfullyStarted => sender() ! SuccessfullyStarted
    case StopConsuming       => handleStopConsuming(state)
    case GetMetrics          => handleGetMetrics(state)
  }

  private def stopping(state: InternalState): Receive = {
    case msg: SuccessfullyStopped =>
      handleSuccessfullyStopped()
      sender() ! msg
    case _ => stash()
  }

  private def startConsumer(): Unit = {
    log.info("Starting consumer for topic {} with client id {}", Seq(topicName, clientId): _*)
    val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)

    val result = RestartSource
      .onFailuresWithBackoff(
        minBackoff = backoffMin,
        maxBackoff = backoffMax,
        randomFactor = randomFactor) { () =>
        log.debug("Creating Kafka source for topic {} with client id {}", Seq(topicName, clientId): _*)
        Consumer
          .committableSource(consumerSettings, subscription)
          .mapMaterializedValue(c => control.set(c))
          .via(businessFlow)
          .map(_.committableOffset)
          .via(Committer.flow(committerSettings))
      }.runWith(Sink.ignore)

    val state = InternalState(control, result)
    context.become(consuming(state))
    result.map(_ => SuccessfullyStarted).pipeTo(self)(sender())
  }

  private def handleStopConsuming(state: InternalState): Unit = {
    val control = state.control.get()
    val drainingControl = DrainingControl(control -> state.streamCompletion)
    log.info("Stopping consumer with client id {} for topic {}", Seq(clientId, topicName): _*)
    context.become(stopping(state))
    drainingControl.drainAndShutdown().map(_ => SuccessfullyStopped(localAddress, self)).pipeTo(self)(sender())
  }

  private def handleSuccessfullyStopped(): Unit = {
    log.info("Consumer with client id {} for topic {} successfully stopped", Seq(clientId, topicName): _*)
    context.become(stopped)
    unstashAll()
  }

  private def handleGetMetrics(state: InternalState): Unit = {
    val control = state.control.get()
    control.metrics pipeTo sender()
  }

  override def preStart(): Unit = {
    registerService(KafkaStreamManager.serviceIdentifier, self, List(topicName))
    super.preStart()
  }
}
