// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import akka.actor.{ Actor, Stash }
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerConfigExtension }
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig }
import surge.config.TimeoutConfig
import surge.kafka.streams.HealthyActor.GetHealth
import surge.kafka.streams.KafkaStreamLifeCycleManagement._
import surge.kafka.streams.KafkaStreamsUncaughtExceptionHandler.KafkaStreamsUncaughtException
import surge.kafka.streams.KafkaStreamsUpdatePartitionsOnStateChangeListener.KafkaStateChange
import surge.metrics.Metrics
import surge.scala.core.kafka.UltiKafkaConsumerConfig
import surge.support.{ Logging, SystemExit, inlineReceive }

import scala.util.{ Failure, Success, Try }

trait KafkaStreamLifeCycleManagement[K, V, T <: KafkaStreamsConsumer[K, V], SV] extends Actor with Stash with Logging {
  val settings: KafkaStreamSettings
  var lastConsumerSeen: Option[T] = None
  val healthCheckName: String

  log.debug(s"Kafka streams ${settings.storeName} cache memory being used is {} KiB", Math.round(settings.cacheMemory.toFloat / 1024f))

  protected val streamsConfig: Map[String, String]
  protected val metrics: Metrics

  private val streamsMetricName = s"kafka-streams-${settings.applicationId}-${settings.storeName}"
  private val config = ConfigFactory.load()
  private val enableMetrics = config.getBoolean("surge.kafka-streams.enable-kafka-metrics")

  /**
   * Base configuration for all streams, extend as needed
   * Ex: override val streamsConfig = baseStreamsConfig ++ Map[String, String](... stream specific config ...)
   */
  val baseStreamsConfig: Map[String, String] = Map[String, String](
    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> TimeoutConfig.Kafka.consumerSessionTimeout.toMillis.toString,
    ConsumerConfigExtension.LEAVE_GROUP_ON_CLOSE_CONFIG -> TimeoutConfig.debugTimeoutEnabled.toString,
    StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG -> settings.cacheMemory.toString)

  protected val stateChangeListener = new KafkaStreamsNotifyOnStateChangeListener(settings.storeName, List(receiveKafkaStreamStateChange))
  protected val uncaughtExceptionListener = new KafkaStreamsUncaughtExceptionHandler(List(receiveUnhandledExceptions))
  protected val stateRestoreListener = new KafkaStreamsStateRestoreListener

  def createConsumer(): T
  def createQueryableStore(consumer: T): KafkaStreamsKeyValueStore[K, SV]
  def initialize(consumer: T): Unit

  // Override to setup child specific messages on each stage
  def uninitialized: Receive = Map.empty
  def created(consumer: T): Receive = Map.empty
  def running(consumer: T, stateStore: KafkaStreamsKeyValueStore[K, SV]): Receive = Map.empty

  // Hook stream specific additional code on start, stop, error, or state change
  def onStart(): Unit = {} // will be called every time the stream starts
  def onStop(consumer: T): Unit = {} // will be called every time the stream stops
  def onStreamUncaughtError(uncaughtException: KafkaStreamsUncaughtException): Unit = {}
  def onStateChange(change: KafkaStateChange): Unit = {}

  final def start(): Unit = {
    val consumer = createConsumer()
    lastConsumerSeen = Some(consumer)
    log.info(s"Kafka streams ${settings.storeName} is created")
    context.become(streamCreated(consumer))

    initialize(consumer)
    subscribeListeners(consumer)

    if (settings.clearStateOnStartup) {
      log.debug(s"Kafka streams ${settings.storeName} clean up on start")
      consumer.streams.cleanUp()
    }
    consumer.start()
    if (enableMetrics) {
      metrics.registerKafkaMetrics(streamsMetricName, () => consumer.streams.metrics())
    }
    onStart() // child specific start operations
  }

  final def stop(consumer: T): Unit = {
    if (enableMetrics) {
      metrics.unregisterKafkaMetric(streamsMetricName)
    }
    Try(consumer.streams.close()) match {
      case Success(_) =>
        log.debug(s"Kafka streams ${settings.storeName} stopped")
      case Failure(error) =>
        log.error(s"Kafka streams ${settings.storeName} failed to stop, shutting down the JVM", error)
        // Let the app crash, dead locks risk if the stream fails to kill itself, its not safe to restart
        SystemExit.exit(1)
    }
    onStop(consumer)
    log.info(s"Kafka streams ${settings.storeName} is uninitialized")
    context.become(streamUninitialized)
  }

  final private def restart(): Unit = {
    self ! Stop
    self ! Start
  }

  /**
   * Be careful when overriding this method, notice that the stream must set the "stateChangeListener"
   * defined here in order to work correctly
   * Use KafkaStreamsStateChangeWithMultipleListeners(stateChangeListener, yourOwnListener)
   * to add your own state change listener
   * @param consumer
   */
  protected def subscribeListeners(consumer: T): Unit = {
    consumer.streams.setStateListener(stateChangeListener)
    consumer.streams.setGlobalStateRestoreListener(stateRestoreListener)
    consumer.streams.setUncaughtExceptionHandler(uncaughtExceptionListener)
  }

  final override def receive: Receive = streamUninitialized

  final def streamUninitialized: Receive = uninitialized orElse inlineReceive {
    case Start =>
      start()
    case GetHealth =>
      sender() ! getHealth(HealthCheckStatus.DOWN)
    case Stop | Restart =>
    // drop, can't restart or stop if is not running
  } orElse errorHandler orElse logUnhandled("uninitialized")

  final def streamCreated(consumer: T): Receive = created(consumer) orElse inlineReceive {
    case Run =>
      val queryableStore: KafkaStreamsKeyValueStore[K, SV] = createQueryableStore(consumer)
      unstashAll()
      log.info(s"Kafka streams ${settings.storeName} is running")
      context.become(streamRunning(consumer, queryableStore))
    case Stop =>
      stop(consumer)
    case Restart =>
      restart()
    case GetHealth =>
      val status = if (consumer.streams.state().isRunningOrRebalancing) HealthCheckStatus.UP else HealthCheckStatus.DOWN
      sender() ! getHealth(status)
  } orElse errorHandler orElse logUnhandled("created")

  final def streamRunning(consumer: T, queryableStore: KafkaStreamsKeyValueStore[K, SV]): Receive = running(consumer, queryableStore) orElse inlineReceive {
    case GetHealth =>
      val status = if (consumer.streams.state().isRunningOrRebalancing) HealthCheckStatus.UP else HealthCheckStatus.DOWN
      sender() ! getHealth(status)
    case Stop =>
      stop(consumer)
    case Restart =>
      restart()
  } orElse errorHandler orElse logUnhandled("running")

  final def logUnhandled(stateName: String): Receive = {
    case unhandledMessage =>
      log.debug(s"${settings.storeName} Dropping unhandled message on $stateName state {}", unhandledMessage)
  }

  final def receiveUnhandledExceptions(uncaughtException: KafkaStreamsUncaughtException): Unit = {
    log.error(s"Kafka stream unhandled exception in ${settings.storeName}, thread ${uncaughtException.thread}", uncaughtException.exception)
    log.debug(s"Kafka stream should transition to ERROR state and be restarted")
    onStreamUncaughtError(uncaughtException)
  }

  final def receiveKafkaStreamStateChange(change: KafkaStateChange): Unit = {
    onStateChange(change)
    change match {
      case KafkaStateChange(_, newState) if newState == KafkaStreams.State.RUNNING =>
        self ! Run
      case KafkaStateChange(_, newState) if newState == KafkaStreams.State.ERROR =>
        restartOnError(new RuntimeException(s"Kafka stream ${settings.storeName} transitioned to ERROR state, crashing this actor to let it restart"))
      case _ =>
      // Ignore
    }
  }

  final private[streams] def errorHandler: Receive = {
    case err: Throwable =>
      log.error("Restarting actor with error", err)
      throw err
  }

  def restartOnError(err: Throwable): Unit = {
    self ! err
  }

  def getHealth(streamStatus: String): HealthCheck = {
    HealthCheck(
      name = healthCheckName,
      id = settings.storeName,
      status = streamStatus)
  }

  /**
   * Actor lifecycle hook, make sure you call super.preStart if you override this method
   */
  override def preStart(): Unit = {
    self ! Start
    super.preStart()
  }

  /**
   * Actor lifecycle hook, make sure you call super.preRestart if you override this method
   */
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    lastConsumerSeen.foreach(stop)
    super.preRestart(reason, message)
  }

  override def postStop(): Unit = {
    lastConsumerSeen.foreach(stop)
    super.postStop()
  }

}

private[streams] object KafkaStreamLifeCycleManagement {
  sealed private[streams] trait KafkaStreamLifeCycleCommand

  case object Start extends KafkaStreamLifeCycleCommand
  case object Restart extends KafkaStreamLifeCycleCommand
  case object Stop extends KafkaStreamLifeCycleCommand
  case object Run extends KafkaStreamLifeCycleCommand
}

private[streams] trait KafkaStreamSettings {
  val storeName: String
  val brokers: Seq[String]
  val consumerConfig: UltiKafkaConsumerConfig
  val applicationId: String
  val cacheMemory: Long
  val clearStateOnStartup: Boolean
}
