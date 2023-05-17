// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern.{ ask, BackoffOpts, BackoffSupervisor }
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.streams.Topology
import surge.core.{ Ack, Controllable }
import surge.health.HealthSignalBusTrait
import surge.internal.config.{ BackoffConfig, TimeoutConfig }
import surge.internal.health.{ HealthCheck, HealthyActor, HealthyComponent }
import surge.internal.utils.{ BackoffChildActorTerminationWatcher, Logging }
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

import java.util.regex.Pattern
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object AggregateStreamsWriteBufferSettings extends WriteBufferSettings {
  private val config = ConfigFactory.load()
  override def maxWriteBufferNumber: Int = config.getInt("kafka.streams.rocks-db.num-write-buffers")
  override def writeBufferSizeMb: Int = config.getInt("kafka.streams.rocks-db.write-buffer-size-mb")
}

object AggregateStreamsBlockCacheSettings extends BlockCacheSettings {
  private val config = ConfigFactory.load()
  override def blockSizeKb: Int = 16
  override def blockCacheSizeMb: Int = config.getInt("kafka.streams.rocks-db.block-cache-size-mb")
  override def cacheIndexAndFilterBlocks: Boolean = true
}

class AggregateStreamsRocksDBConfig extends CustomRocksDBConfigSetter(AggregateStreamsBlockCacheSettings, AggregateStreamsWriteBufferSettings)

/**
 * Creates a state store exposed as a Kafka Streams KTable for a particular Kafka Topic. The state in the KTable are key value pairs based on the Kafka Key and
 * Kafka Value for each record read from the given topic.
 *
 * @param aggregateName
 *   Name of the aggregate being consumed. Used to define consumer group and the name of the state store backing the KTable.
 * @param stateTopic
 *   The topic of state key/value pairs to consume and expose via a KTable. This topic should be compacted.
 * @param partitionTrackerProvider
 *   Registered within a Kafka Streams state change listener to track updates to the Kafka Streams consumer group. When the consumer group transitions from
 *   rebalancing to running, the partition tracker provided will be notified automatically. This can be used for notifying other processes/interested parties
 *   that a consumer group change has occurred.
 * @param applicationHostPort
 *   Optional string to use for a host/port exposed by this application. This information is exposed to the partition tracker provider as mappings from
 *   application host/port to assigned partitions.
 */
class AggregateStateStoreKafkaStreams(
    aggregateName: String,
    stateTopic: KafkaTopic,
    partitionTrackerProvider: KafkaStreamsPartitionTrackerProvider,
    applicationHostPort: Option[String],
    val applicationId: String,
    clientId: String,
    signalBus: HealthSignalBusTrait,
    system: ActorSystem,
    metrics: Metrics,
    config: Config)(implicit ec: ExecutionContext)
    extends HealthyComponent
    with Logging {

  private[streams] lazy val settings = SurgeAggregateStoreSettings(config, applicationId, aggregateName, clientId, applicationHostPort)
  private lazy val kStreamConsumer = new SurgeStateStoreConsumer(stateTopic, settings, config)

  private[streams] val underlyingActor = createUnderlyingActorWithBackOff()

  private implicit val askTimeoutDuration: Timeout = TimeoutConfig.StateStoreKafkaStreamActor.askTimeout

  override def restartSignalPatterns(): Seq[Pattern] = {
    Seq(Pattern.compile("kafka.streams.fatal.error"))
  }

  /**
   * Used to actually start the Kafka Streams process. Optionally cleans up persistent state directories left behind by previous runs if
   * `kafka.streams.wipe-state-on-start` config setting is set to true.
   */

  def getAggregateBytes(aggregateId: String): Future[Option[Array[Byte]]] = {
    underlyingActor.ask(KafkaStreamManagerActor.GetAggregateBytes(aggregateId)).mapTo[Option[Array[Byte]]]
  }

  override def healthCheck(): Future[HealthCheck] = {
    underlyingActor.ask(HealthyActor.GetHealth).mapTo[HealthCheck]
  }

  def substatesForAggregate(aggregateId: String)(implicit ec: ExecutionContext): Future[List[(String, Array[Byte])]] = {
    // FIXME Is this actually still a useful concept? In practice nobody is using this since it's not actually exposed
    // underlyingActor.ask(KafkaStreamManagerActor.GetSubstatesForAggregate(aggregateId)).mapTo[List[(String, Array[Byte])]]
    Future.failed(new UnsupportedOperationException("substatesForAggregate is not supported"))
  }

  private def createUnderlyingActorWithBackOff(): ActorRef = {
    def onMaxRetries(): Unit = {
      log.error(s"Kafka stream ${settings.storeName} failed more than the max number of retries, Surge is killing the JVM")
      System.exit(1)
    }

    val aggregateStateStoreKafkaStreamsImplProps =
      KafkaStreamManagerActor.props(kStreamConsumer, partitionTrackerProvider, metrics)

    val underlyingActorProps = BackoffSupervisor.props(
      BackoffOpts
        .onStop(
          aggregateStateStoreKafkaStreamsImplProps,
          childName = settings.storeName,
          minBackoff = BackoffConfig.StateStoreKafkaStreamActor.minBackoff,
          maxBackoff = BackoffConfig.StateStoreKafkaStreamActor.maxBackoff,
          randomFactor = BackoffConfig.StateStoreKafkaStreamActor.randomFactor)
        .withMaxNrOfRetries(BackoffConfig.StateStoreKafkaStreamActor.maxRetries))

    val underlyingCreatedActor = system.actorOf(underlyingActorProps)

    system.actorOf(BackoffChildActorTerminationWatcher.props(underlyingCreatedActor, () => onMaxRetries()))

    underlyingCreatedActor
  }

  private[streams] def getTopology: Future[Topology] = {
    Future.successful(kStreamConsumer.ktableIndexingTopology)
  }

  private def registrationCallback(): PartialFunction[Try[Ack], Unit] = {
    case Success(_) =>
      val registrationResult = signalBus.register(
        control = controllable,
        componentName = s"state-store-kafka-streams-$aggregateName",
        shutdownSignalPatterns = shutdownSignalPatterns(),
        restartSignalPatterns = restartSignalPatterns())

      registrationResult.onComplete {
        case Failure(exception) =>
          log.error(s"$getClass registration failed", exception)
        case Success(_) =>
          log.debug(s"$getClass registration succeeded")
      }(system.dispatcher)
    case Failure(error) =>
      log.error(s"Unable to register $getClass for supervision", error)
  }
  private def unregistrationCallback(): PartialFunction[Try[Ack], Unit] = {
    case Success(_) =>
      val unRegistrationResult = signalBus.unregister(control = controllable, componentName = s"state-store-kafka-streams-$aggregateName")

      unRegistrationResult.onComplete {
        case Failure(exception) =>
          log.error(s"$getClass registration failed", exception)
        case Success(_) =>
          log.debug(s"$getClass registration succeeded")
      }(system.dispatcher)
    case Failure(exception) =>
      log.error("Failed to stop so unable to unregister from supervision", exception)
  }

  override val controllable: Controllable = new Controllable {
    override def start(): Future[Ack] = {
      underlyingActor.ask(KafkaStreamManagerActor.Start).mapTo[Ack].andThen(registrationCallback())
    }

    override def restart(): Future[Ack] = {
      for {
        _ <- stop()
        started <- start()
      } yield {
        started
      }
    }

    override def stop(): Future[Ack] = {
      underlyingActor.ask(KafkaStreamManagerActor.Stop).mapTo[Ack].andThen(unregistrationCallback())
    }

    override def shutdown(): Future[Ack] = stop()
  }
}
