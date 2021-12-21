// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde
import org.apache.kafka.streams.{ KafkaStreams, StoreQueryParameters, StreamsConfig, Topology }
import org.apache.kafka.streams.kstream.{ Consumed, Materialized }
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.slf4j.LoggerFactory
import surge.kafka.{ KafkaSecurityConfiguration, KafkaTopic }

import java.util.Properties
import scala.concurrent.ExecutionContext

private[surge] class SurgeStateStoreConsumer(stateTopic: KafkaTopic, val settings: SurgeAggregateStoreSettings, val config: Config)
    extends KafkaSecurityConfiguration {

  private val log = LoggerFactory.getLogger(getClass)

  private implicit val consumedKeyValue: Consumed[String, Array[Byte]] = Consumed.`with`(Serdes.String(), Serdes.ByteArray())

  private val persistencePlugin = SurgeKafkaStreamsPersistencePluginLoader.load(config)

  private val topologyProps = new Properties()
  topologyProps.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE)

  log.debug(s"Kafka streams ${settings.storeName} cache memory being used is {} KiB", Math.round(settings.cacheMemory.toFloat / 1024f))

  private val streamsConfig: Map[String, String] = Map(
    StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG -> settings.cacheMemory.toString,
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> settings.brokers.mkString(","),
    StreamsConfig.APPLICATION_ID_CONFIG -> settings.applicationId,
    ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed",
    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG -> Integer.MAX_VALUE.toString,
    StreamsConfig.CLIENT_ID_CONFIG -> settings.clientId,
    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> settings.commitInterval.toString,
    StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG -> settings.standByReplicas.toString,
    StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG -> StreamsConfig.OPTIMIZE,
    StreamsConfig.STATE_DIR_CONFIG -> settings.stateDirectory,
    StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG -> classOf[AggregateStreamsRocksDBConfig].getName,
    StreamsConfig.APPLICATION_SERVER_CONFIG -> settings.applicationHostPort.getOrElse(settings.localActorHostPort))

  private val ktableStreamProps: Properties = {
    val p = new Properties()
    streamsConfig.foreach(propPair => p.put(propPair._1, propPair._2))
    configureSecurityProperties(p)
    p
  }

  val storeName: String = settings.storeName

  val ktableIndexingTopology: Topology = {
    val builder: StreamsBuilder = new StreamsBuilder

    val aggregateStoreMaterializedBase =
      Materialized.as[String, Array[Byte]](persistencePlugin.createSupplier(settings.storeName)).withValueSerde(new ByteArraySerde())

    val aggregateStoreMaterialized = if (!persistencePlugin.enableLogging) {
      aggregateStoreMaterializedBase.withLoggingDisabled()
    } else {
      aggregateStoreMaterializedBase
    }

    builder.table(stateTopic.name, aggregateStoreMaterialized)

    if (persistencePlugin.enableLogging) {
      builder.build(topologyProps)
    } else {
      builder.build()
    }
  }

  def createQueryableStore(consumer: KafkaStreams)(implicit ec: ExecutionContext): SurgeAggregateStore = {
    val storeParams = StoreQueryParameters.fromNameAndType(settings.storeName, QueryableStoreTypes.keyValueStore[String, Array[Byte]]())
    val underlying = consumer.store(storeParams)
    new SurgeAggregateStore(settings.storeName, new KafkaStreamsKeyValueStore(underlying))
  }

  def createConsumer(): KafkaStreams = {
    new KafkaStreams(ktableIndexingTopology, ktableStreamProps)
  }
}

case class SurgeAggregateStoreSettings(
    storeName: String,
    brokers: Seq[String],
    applicationId: String,
    clientId: String,
    cacheMemory: Long,
    standByReplicas: Int,
    commitInterval: Int,
    stateDirectory: String,
    clearStateOnStartup: Boolean,
    enableMetrics: Boolean,
    applicationHostPort: Option[String],
    localActorHostPort: String)
object SurgeAggregateStoreSettings {
  def apply(
      config: Config,
      applicationId: String,
      aggregateName: String,
      clientId: String,
      applicationHostPort: Option[String]): SurgeAggregateStoreSettings = {
    val aggregateStateStoreName: String = s"${aggregateName}AggregateStateStore"
    val brokers = config.getString("kafka.brokers").split(",").toVector
    val cacheHeapPercentage = config.getDouble("kafka.streams.cache-heap-percentage")
    val totalMemory = Runtime.getRuntime.maxMemory()
    val cacheMemory = (totalMemory * cacheHeapPercentage).longValue
    val standbyReplicas = config.getInt("kafka.streams.num-standby-replicas")
    val commitInterval = config.getInt("kafka.streams.commit-interval-ms")
    val stateDirectory = config.getString("kafka.streams.state-dir")
    val clearStateOnStartup = config.getBoolean("kafka.streams.wipe-state-on-start")
    val enableMetrics = config.getBoolean("surge.kafka-streams.enable-kafka-metrics")
    val localActorProviderHostPort = config.getString("surge.local-actor-provider.host-port")

    SurgeAggregateStoreSettings(
      storeName = aggregateStateStoreName,
      brokers = brokers,
      applicationId = applicationId,
      clientId = clientId,
      cacheMemory = cacheMemory,
      standByReplicas = standbyReplicas,
      commitInterval = commitInterval,
      stateDirectory = stateDirectory,
      clearStateOnStartup = clearStateOnStartup,
      enableMetrics = enableMetrics,
      applicationHostPort = applicationHostPort,
      localActorHostPort = localActorProviderHostPort)
  }
}
