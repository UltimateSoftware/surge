// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import akka.actor.{ Actor, NoSerializationVerificationNeeded, Props, Stash }
import akka.pattern.pipe
import org.apache.kafka.streams.KafkaStreams
import org.slf4j.LoggerFactory
import surge.core.Ack
import surge.internal.health.{ HealthCheck, HealthCheckStatus }
import surge.internal.health.HealthyActor.GetHealth
import surge.kafka.streams.KafkaStreamsUncaughtExceptionHandler.KafkaStreamsUncaughtException
import surge.kafka.streams.KafkaStreamsUpdatePartitionsOnStateChangeListener.KafkaStateChange
import surge.metrics.Metrics

import java.time.Duration
import java.time.temporal.ChronoUnit
import scala.util.{ Failure, Success, Try }

private[surge] object KafkaStreamManagerActor {
  sealed trait StreamManagerCommand extends NoSerializationVerificationNeeded

  case object Start extends StreamManagerCommand
  case object Stop extends StreamManagerCommand
  case object Run extends StreamManagerCommand
  case class GetAggregateBytes(aggregateId: String)

  sealed trait InternalMessage extends StreamManagerCommand
  case class KafkaStreamError(error: Throwable) extends InternalMessage

  sealed trait StreamManagerResponse extends NoSerializationVerificationNeeded

  def props(surgeConsumer: SurgeStateStoreConsumer, partitionTrackerProvider: KafkaStreamsPartitionTrackerProvider, metrics: Metrics): Props = {
    Props(new KafkaStreamManagerActor(surgeConsumer, partitionTrackerProvider, metrics))
  }
}

class KafkaStreamManagerActor(surgeConsumer: SurgeStateStoreConsumer, partitionTrackerProvider: KafkaStreamsPartitionTrackerProvider, metrics: Metrics)
    extends Actor
    with Stash {
  import KafkaStreamManagerActor._
  import context.dispatcher

  private var lastConsumerSeen: Option[KafkaStreams] = None

  private val log = LoggerFactory.getLogger(getClass)

  private val streamsMetricName: String = s"kafka-streams-${surgeConsumer.settings.applicationId}-${surgeConsumer.settings.storeName}"
  private val healthCheckName: String = "aggregate-state-store"

  override def receive: Receive = stopped

  private def stopped: Receive = {
    case Start =>
      start()
      sender() ! Ack()
    case GetHealth =>
      sender() ! getHealth(HealthCheckStatus.DOWN)
    case Stop =>
      sender() ! Ack()
    case KafkaStreamError(t) =>
      handleError(t)
  }

  private def starting(stream: KafkaStreams): Receive = {
    case Run =>
      val queryableStore = surgeConsumer.createQueryableStore(stream)
      unstashAll()
      log.info(s"Kafka streams ${surgeConsumer.storeName} is running")
      context.become(running(stream, queryableStore))
    case Stop =>
      stop(stream)
      sender() ! Ack()
    case GetHealth =>
      val status = if (stream.state().isRunningOrRebalancing) HealthCheckStatus.UP else HealthCheckStatus.DOWN
      sender() ! getHealth(status)
    case KafkaStreamError(t) =>
      handleError(t)
  }

  private val actorStateChangeListener = new KafkaStreamsNotifyOnStateChangeListener(surgeConsumer.storeName, List(receiveKafkaStreamStateChange))
  private val uncaughtExceptionListener = new KafkaStreamsUncaughtExceptionHandler(List(receiveUnhandledExceptions))
  private val stateRestoreListener = new KafkaStreamsStateRestoreListener

  private def running(stream: KafkaStreams, stateStore: SurgeAggregateStore): Receive = {
    case GetAggregateBytes(aggregateId) =>
      stateStore.getAggregateBytes(aggregateId).pipeTo(sender())
    case GetHealth =>
      val status = if (stream.state().isRunningOrRebalancing) HealthCheckStatus.UP else HealthCheckStatus.DOWN
      sender() ! getHealth(status)
    case Stop =>
      stop(stream)
      sender() ! Ack()
    case Start =>
      sender() ! Ack()
    case KafkaStreamError(t) =>
      handleError(t)
  }

  private def start(): Unit = {
    val consumer = surgeConsumer.createConsumer()
    log.info(s"Kafka streams ${surgeConsumer.storeName} is created")
    lastConsumerSeen = Some(consumer)
    context.become(starting(consumer))

    val externalPartitionTrackerListener =
      new KafkaStreamsUpdatePartitionsOnStateChangeListener(surgeConsumer.storeName, partitionTrackerProvider.create(consumer), false)
    val stateChangeListener = new KafkaStreamsStateChangeWithMultipleListeners(actorStateChangeListener, externalPartitionTrackerListener)

    consumer.setStateListener(stateChangeListener)
    consumer.setGlobalStateRestoreListener(stateRestoreListener)
    consumer.setUncaughtExceptionHandler(uncaughtExceptionListener)

    if (surgeConsumer.settings.clearStateOnStartup) {
      log.debug(s"Kafka streams ${surgeConsumer.storeName} clean up on start")
      consumer.cleanUp()
    }

    consumer.start()
    sys.ShutdownHookThread {
      consumer.close(Duration.of(10, ChronoUnit.SECONDS))
    }

    if (surgeConsumer.settings.enableMetrics) {
      metrics.registerKafkaMetrics(streamsMetricName, () => consumer.metrics())
    }
  }

  private def stop(consumer: KafkaStreams): Unit = {
    if (surgeConsumer.settings.enableMetrics) {
      metrics.unregisterKafkaMetric(streamsMetricName)
    }
    Try(consumer.close()) match {
      case Success(_) =>
        log.debug(s"Kafka streams ${surgeConsumer.storeName} stopped")
      case Failure(error) =>
        log.error(s"Kafka streams ${surgeConsumer.storeName} failed to stop, shutting down the JVM", error)
        // Let the app crash, dead locks risk if the stream fails to kill itself, its not safe to restart
        System.exit(1)
    }
    log.info(s"Kafka streams ${surgeConsumer.storeName} is stopped")
    lastConsumerSeen = None
    context.become(stopped)
  }

  private def receiveUnhandledExceptions(uncaughtException: KafkaStreamsUncaughtException): Unit = {
    log.error(s"Kafka stream unhandled exception in ${surgeConsumer.storeName}, thread ${uncaughtException.thread}", uncaughtException.exception)
    log.debug(s"Kafka stream should transition to ERROR state and be restarted")
  }

  private def receiveKafkaStreamStateChange(change: KafkaStateChange): Unit = {
    change match {
      case KafkaStateChange(_, newState) if newState == KafkaStreams.State.RUNNING =>
        self ! Run
      case KafkaStateChange(_, newState) if newState == KafkaStreams.State.ERROR =>
        restartOnError(new RuntimeException(s"Kafka stream ${surgeConsumer.storeName} transitioned to ERROR state, crashing this actor to let it restart"))
      case _ =>
      // Ignore
    }
  }

  private def handleError(err: Throwable): Unit = {
    log.error("Restarting actor with error", err)
    throw err
  }

  def restartOnError(err: Throwable): Unit = {
    self ! KafkaStreamError(err)
  }

  private def getHealth(streamStatus: String): HealthCheck = {
    HealthCheck(name = healthCheckName, id = surgeConsumer.storeName, status = streamStatus)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    lastConsumerSeen.foreach(stop)
    super.preRestart(reason, message)
  }

  override def postStop(): Unit = {
    lastConsumerSeen.foreach(stop)
    super.postStop()
  }
}
