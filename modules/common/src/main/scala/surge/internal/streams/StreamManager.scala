// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import akka.actor.{ Actor, ActorRef, ActorSystem, Address, NoSerializationVerificationNeeded, Props, Stash }
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ ConsumerMessage, _ }
import akka.pattern._
import akka.stream.scaladsl.{ Flow, RestartSource, Sink }
import akka.util.Timeout
import akka.{ Done, NotUsed }
import com.typesafe.config.ConfigFactory
import io.opentelemetry.api.trace.Tracer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.{ Metric, MetricName }
import org.slf4j.{ LoggerFactory, MDC }
import surge.internal.akka.ActorWithTracing
import surge.internal.akka.cluster.{ ActorHostAwareness, ActorRegistry, ActorSystemHostAwareness }
import surge.internal.kafka.{ HeadersHelper, HostAwareCooperativeStickyAssignor, HostAwareRangeAssignor }
import surge.internal.utils.Logging
import surge.streams.DataPipeline._
import surge.streams.replay.{ EventReplaySettings, EventReplayStrategy, ReplayControl, ReplayControlContext }
import surge.streams.{ EventPlusStreamMeta, KafkaStreamMeta }

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.Try

object PartitionAssignorConfig {
  private val config = ConfigFactory.load()
  private val log = LoggerFactory.getLogger(getClass)
  val assignorClassName: String = config.getString("surge.kafka-event-source.consumer.partition-assignor").toLowerCase() match {
    case "range"              => classOf[HostAwareRangeAssignor].getName
    case "cooperative-sticky" => classOf[HostAwareCooperativeStickyAssignor].getName
    case other =>
      log.warn("Attempted to use an unknown range assignor named [{}], accepted values are [range, cooperative-sticky].  Defaulting to range.", other)
      classOf[HostAwareRangeAssignor].getName
  }
}

object KafkaStreamManager {
  val serviceIdentifier = "StreamManager"

  def wrapBusinessFlow[Key, Value](business: Flow[EventPlusStreamMeta[Key, Value, KafkaStreamMeta], KafkaStreamMeta, NotUsed])
      : Flow[ConsumerMessage.CommittableMessage[Key, Value], KafkaStreamMeta, NotUsed] = {
    val contextToPropagate = MDC.getCopyOfContextMap

    Flow[ConsumerMessage.CommittableMessage[Key, Value]]
      .map { msg =>
        Option(contextToPropagate).foreach(contextToPropagate => MDC.setContextMap(contextToPropagate))

        val kafkaMeta = KafkaStreamMeta(msg.record.topic(), msg.record.partition(), msg.record.offset(), msg.committableOffset)

        MDC.put("kafka.topic", kafkaMeta.topic)
        MDC.put("kafka.partition", kafkaMeta.partition.toString)
        MDC.put("kafka.offset", kafkaMeta.offset.toString)
        EventPlusStreamMeta(msg.record.key, msg.record.value, kafkaMeta, HeadersHelper.unapplyHeaders(msg.record.headers()))
      }
      .via(business)
      .map { meta =>
        MDC.clear()
        meta
      }
  }
}

object MetricsWrapper {
  def apply(metrics: Future[Map[MetricName, Metric]])(implicit ex: ExecutionContext): Future[MetricsWrapper] = metrics.map(new MetricsWrapper(_))
  lazy val empty: MetricsWrapper = new MetricsWrapper(Map.empty)
}
class MetricsWrapper(val metrics: Map[MetricName, Metric]) extends NoSerializationVerificationNeeded

class KafkaStreamManager[Key, Value](
    topicName: String,
    consumerSettings: ConsumerSettings[Key, Value],
    subscriptionProvider: KafkaSubscriptionProvider[Key, Value],
    keyDeserializer: Deserializer[Key],
    valueDeserializer: Deserializer[Value],
    replayStrategy: EventReplayStrategy,
    replaySettings: EventReplaySettings,
    val tracer: Tracer)(implicit val actorSystem: ActorSystem)
    extends ActorSystemHostAwareness
    with Logging {

  private val config = ConfigFactory.load()
  private val metricFetchTimeout = config.getDuration("surge.kafka-event-source.kafka-metric-fetch-timeout").toMillis.milliseconds
  private val consumerGroup = consumerSettings.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
  private val actorRegistry = new ActorRegistry(actorSystem)
  private val managerActor = actorSystem.actorOf(Props(new KafkaStreamManagerActor(topicName, subscriptionProvider, tracer, actorRegistry)))

  private val deserializeKey: Array[Byte] => Key = { bytes => keyDeserializer.deserialize(topicName, bytes) }
  private val deserializeValue: Array[Byte] => Value = { bytes => valueDeserializer.deserialize(topicName, bytes) }
  private val replayContext =
    ReplayControlContext(keyDeserializer = deserializeKey, valueDeserializer = deserializeValue, dataHandler = subscriptionProvider.businessFlow)
  private val replayControl = replayStrategy.createReplayController(replayContext)
  private val replayCoordinator =
    actorSystem.actorOf(Props(new ReplayCoordinator(topicName, consumerGroup, actorRegistry, replayControl)))

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
    (managerActor ? KafkaStreamManagerActor.GetMetrics).mapTo[MetricsWrapper].map(_.metrics)(global)
  }

  // TODO is this Await too expensive? Akka streams only exposes the Kafka metrics from a future even though the Kafka client interfaces expose it non-async
  def getMetricsSynchronous: Map[MetricName, Metric] = {
    Try(Await.result(getMetrics, metricFetchTimeout)).getOrElse(Map.empty)
  }

  def replay(): Future[ReplayResult] = {
    implicit val entireProcessTimeout: Timeout = Timeout(replaySettings.entireReplayTimeout)
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    (replayCoordinator ? ReplayCoordinator.StartReplay)
      .map {
        case ReplayCoordinator.ReplayCompleted =>
          ReplaySuccessfullyStarted()
        case ReplayCoordinator.ReplayFailed(err) =>
          ReplayFailed(err)
      }
      .recoverWith { case err: Throwable =>
        log.error(
          s"An unexpected error happened replaying $consumerGroup, " +
            s"please try again, if the problem persists, reach out the Surge team for support",
          err)
        replayCoordinator ! ReplayCoordinator.StopReplay
        Future.successful(ReplayFailed(err))
      }
  }

  def getReplayControl: ReplayControl = replayControl
}

object KafkaStreamManagerActor {
  type OnReplayComplete = (String, Long) => Unit

  sealed trait KafkaStreamManagerActorRequest
  case object StartConsuming extends KafkaStreamManagerActorRequest
  case object StopConsuming extends KafkaStreamManagerActorRequest
  case object GetMetrics extends KafkaStreamManagerActorRequest

  sealed trait KafkaStreamManagerActorResponse
  case class SuccessfullyStopped(address: Address, actor: ActorRef) extends KafkaStreamManagerActorResponse
  case object SuccessfullyStarted extends KafkaStreamManagerActorResponse
}

class KafkaStreamManagerActor[Key, Value](
    topicName: String,
    subscriptionProvider: KafkaSubscriptionProvider[Key, Value],
    val tracer: Tracer,
    registry: ActorRegistry)
    extends Actor
    with ActorWithTracing
    with ActorHostAwareness
    with Stash
    with Logging {
  import KafkaStreamManagerActor._
  import context.{ dispatcher, system }

  private val config = ConfigFactory.load()
  private val reuseConsumerId = config.getBoolean("surge.kafka-reuse-consumer-id")
  // Set this uniquely per manager actor so that restarts of the Kafka stream don't cause a rebalance of the consumer group
  private val clientId = s"surge-event-source-managed-consumer-${UUID.randomUUID()}"

  private val backoffMin = config.getDuration("surge.kafka-event-source.backoff.min").toMillis.millis
  private val backoffMax = config.getDuration("surge.kafka-event-source.backoff.max").toMillis.millis
  private val randomFactor = config.getDouble("surge.kafka-event-source.backoff.random-factor")

  private sealed trait InternalMessage
  private case object RegisterSelf extends InternalMessage

  private case class InternalState(control: AtomicReference[Consumer.Control], streamCompletion: Future[Done])

  override def receive: Receive = stopped

  private def stopped: Receive = {
    case StartConsuming => startConsumer()
    case StopConsuming  => sender() ! SuccessfullyStopped(localAddress, self)
    case GetMetrics     => sender() ! MetricsWrapper.empty
    case RegisterSelf   => registerSelf()
  }

  private def consuming(state: InternalState): Receive = {
    case SuccessfullyStarted => sender() ! SuccessfullyStarted
    case StopConsuming       => handleStopConsuming(state)
    case GetMetrics          => handleGetMetrics(state)
    case RegisterSelf        => registerSelf()
  }

  private def stopping(state: InternalState): Receive = {
    case msg: SuccessfullyStopped =>
      handleSuccessfullyStopped()
      sender() ! msg
    case RegisterSelf => registerSelf()
    case _            => stash()
  }

  private def startConsumer(): Unit = {
    log.info("Starting consumer for topic {} with client id {}", Seq(topicName, clientId): _*)
    val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)

    val result = RestartSource
      .onFailuresWithBackoff(minBackoff = backoffMin, maxBackoff = backoffMax, randomFactor = randomFactor) { () =>
        subscriptionProvider.createSubscription(context.system).mapMaterializedValue(c => control.set(c))
      }
      .runWith(Sink.ignore)

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
    MetricsWrapper(control.metrics).pipeTo(sender())
  }

  private def registerSelf(): Unit = {
    log.debug(s"Registering StreamManager at [{}] to the ActorRegistry", self.path)
    registry.registerService(KafkaStreamManager.serviceIdentifier, self, List(topicName)).recover { case _ =>
      self ! RegisterSelf
    }
  }

  override def preStart(): Unit = {
    registerSelf()
    super.preStart()
  }
}
