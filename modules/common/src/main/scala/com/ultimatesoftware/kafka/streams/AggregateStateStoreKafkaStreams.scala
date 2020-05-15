// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.config.TimeoutConfig
import com.ultimatesoftware.scala.core.kafka.{ KafkaTopic, UltiKafkaConsumerConfig }
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerConfigExtension }
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.internals.{ KTableImpl, KTableImplExtensions }
import org.apache.kafka.streams.scala.ByteArrayKeyValueStore
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

object AggregateStreamsWriteBufferSettings extends WriteBufferSettings {
  override def maxWriteBufferNumber: Int = 2
  override def writeBufferSizeMb: Int = 32
}

object AggregateStreamsBlockCacheSettings extends BlockCacheSettings {
  override def blockSizeKb: Int = 16
  override def blockCacheSizeMb: Int = 16
  override def cacheIndexAndFilterBlocks: Boolean = true
}

class AggregateStreamsRocksDBConfig extends CustomRocksDBConfigSetter(AggregateStreamsBlockCacheSettings, AggregateStreamsWriteBufferSettings)

/**
 * Creates a state store exposed as a Kafka Streams KTable for a particular Kafka Topic.  The state in the KTable are
 * key value pairs based on the Kafka Key and Kafka Value for each record read from the given topic.
 *
 * @param aggregateName Name of the aggregate being consumed.  Used to define consumer group and the name of the state store backing the KTable.
 * @param stateTopic The topic of state key/value pairs to consume and expose via a KTable.  This topic should be compacted.
 * @param partitionTrackerProvider Registered within a Kafka Streams state change listener to track updates to the Kafka Streams consumer group.
 *                                 When the consumer group transitions from rebalancing to running, the partition tracker provided will be notified
 *                                 automatically.  This can be used for notifying other processes/interested parties that a consumer group change has occurred.
 * @param kafkaStateMetadataHandler Once records from the aggregate state topic are fully processed (stored in the KTable) this handler is invoked
 *                                  with metadata about the record processed.  This can be used to notify external entities of progress being made through
 *                                  the aggregate topic.
 * @param aggregateValidator Validation function used for each consumed message from Kafka to check if a change from previous aggregate state to new
 *                           aggregate state is valid or not.  Just emits a warning if the change is not valid.
 * @param applicationHostPort Optional string to use for a host/port exposed by this application.  This information is exposed to the partition tracker
 *                            provider as mappings from application host/port to assigned partitions.
 * @tparam Agg Aggregate type being read from Kafka - business logic type of the Kafka values in the state topic
 */
class AggregateStateStoreKafkaStreams[Agg >: Null](
    aggregateName: String,
    stateTopic: KafkaTopic,
    partitionTrackerProvider: KafkaStreamsPartitionTrackerProvider,
    kafkaStateMetadataHandler: KafkaPartitionMetadataHandler,
    aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) ⇒ Boolean,
    applicationHostPort: Option[String],
    consumerGroupName: String) extends HealthyComponent {

  import DefaultSerdes._
  import ImplicitConversions._
  import KTableImplExtensions._

  private val log = LoggerFactory.getLogger(getClass)

  private val config = ConfigFactory.load()
  private val brokers = config.getString("kafka.brokers").split(",")
  private val consumerConfig = UltiKafkaConsumerConfig(consumerGroupName)
  private val environment = config.getString("app.environment")
  private val applicationId = consumerConfig.consumerGroup match {
    case name: String if name.contains(s"-$environment") ⇒
      s"${consumerConfig.consumerGroup}-$aggregateName"
    case _ ⇒
      s"${consumerConfig.consumerGroup}-$aggregateName-$environment"
  }

  private val cacheHeapPercentage = config.getDouble("kafka.streams.cache-heap-percentage")
  private val totalMemory = Runtime.getRuntime.maxMemory()
  private val cacheMemory = (totalMemory * cacheHeapPercentage).longValue
  log.debug("Kafka streams cache memory being used is {} bytes", cacheMemory)
  private val standbyReplicas = config.getInt("kafka.streams.num-standby-replicas")
  private val commitInterval = config.getInt("kafka.streams.commit-interval-ms")
  private val clearStateOnStartup = config.getBoolean("kafka.streams.wipe-state-on-start")

  private val streamsConfig = Map(
    ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed",
    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG -> Integer.MAX_VALUE.toString,
    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> TimeoutConfig.Kafka.consumerSessionTimeout.toMillis.toString,
    ConsumerConfigExtension.LEAVE_GROUP_ON_CLOSE_CONFIG -> TimeoutConfig.debugTimeoutEnabled.toString,
    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> commitInterval.toString,
    StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG -> standbyReplicas.toString,
    StreamsConfig.TOPOLOGY_OPTIMIZATION -> StreamsConfig.OPTIMIZE,
    StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG -> classOf[AggregateStreamsRocksDBConfig].getName,
    StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG -> cacheMemory.toString)

  private val topologyProps = new Properties()
  topologyProps.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE)

  private val consumer = KafkaByteStreamsConsumer(brokers = brokers, applicationId = applicationId, consumerConfig = consumerConfig,
    kafkaConfig = streamsConfig, applicationServerConfig = applicationHostPort, topologyProps = Some(topologyProps))

  private val validationProcessor = new ValidationProcessor[Array[Byte]](aggregateName, aggregateValidator)

  val aggregateStateStore: String = s"${aggregateName}AggregateStateStore"

  private def initStreams(): Unit = {
    val aggregateStoreMaterialized = Materialized.as[String, Array[Byte], ByteArrayKeyValueStore](aggregateStateStore)
      .withValueSerde(new ByteArraySerde())

    // Build the KTable directly from Kafka
    val aggKTable = consumer.builder.table(stateTopic.name, aggregateStoreMaterialized)

    // Reach into the underlying implementation and tell it to send the old value for an aggregate
    // along with the newly updated value.  The old value is only used for validation
    val aggKTableJavaImpl = aggKTable.inner.asInstanceOf[KTableImpl[String, Array[Byte], Array[Byte]]]
    aggKTableJavaImpl.sendOldValues()

    // Run business logic validation and transform values from an aggregate into metadata about
    // the processed record including topic/partition, and offset of the message in Kafka
    val stateMeta = aggKTableJavaImpl.toStreamWithChanges.transformValues(validationProcessor.supplier)
    kafkaStateMetadataHandler.processPartitionMetadata(stateMeta)

    consumer.streams.setStateListener(new KafkaStreamsStateChangeListener(partitionTrackerProvider.create(streams)))
    consumer.streams.setGlobalStateRestoreListener(new KafkaStreamsStateRestoreListener)
    consumer.streams.setUncaughtExceptionHandler(new KafkaStreamsUncaughtExceptionHandler)
  }

  lazy val streams: KafkaStreams = consumer.streams

  def createTopology(): Topology = {
    initStreams()
    kafkaStateMetadataHandler.initialize()
    consumer.topology
  }

  /**
   * Used to actually start the Kafka Streams process.  Optionally cleans up persistent state directories
   * left behind by previous runs if `kafka.streams.wipe-state-on-start` config setting is set to true.
   */
  def start(): Unit = {
    createTopology()
    if (clearStateOnStartup) {
      consumer.streams.cleanUp()
    }
    consumer.start()
    kafkaStateMetadataHandler.start()
  }

  override def healthCheck(): Future[HealthCheck] = Future {
    HealthCheck(
      name = "aggregate-state-store",
      id = aggregateStateStore,
      status = if (streams.state().isRunning) HealthCheckStatus.UP else HealthCheckStatus.DOWN)
  }(ExecutionContext.global)

  /**
   * Asynchronous interface to the key/value store of aggregates built by the stream
   */
  lazy val aggregateQueryableStateStore: KafkaStreamsKeyValueStore[String, Array[Byte]] = {
    val underlying = streams.store(aggregateStateStore, QueryableStoreTypes.keyValueStore[String, Array[Byte]]())
    new KafkaStreamsKeyValueStore(underlying)
  }

  def substatesForAggregate(aggregateId: String)(implicit ec: ExecutionContext): Future[List[(String, Array[Byte])]] = {
    val unfiltered = aggregateQueryableStateStore.range(aggregateId, s"$aggregateId:~")
    unfiltered.map { result ⇒
      result.filter {
        case (key, _) ⇒
          val keyBeforeColon = key.takeWhile(_ != ':')
          keyBeforeColon == aggregateId
      }
    }
  }
}
