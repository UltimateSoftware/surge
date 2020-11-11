// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, Timer }
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }
import org.slf4j.LoggerFactory
import surge.kafka.streams.{ AggregateStreamsRocksDBConfig, KafkaByteStreamsConsumer, KafkaStreamsKeyValueStore }
import surge.scala.core.kafka.{ KafkaTopic, UltiKafkaConsumerConfig }

import scala.util.Try

class SurgeEventProcessor[Agg, Event](
    aggregateName: String,
    readFormatting: SurgeReadFormatting[Agg, Event],
    writeFormatting: SurgeAggregateWriteFormatting[Agg],
    eventsTopic: KafkaTopic,
    applicationHostPort: Option[String],
    extractAggregateId: Event ⇒ Option[String],
    processEvent: (Option[Agg], Event) ⇒ Option[Agg],
    metricsProvider: MetricsProvider) {

  private val log = LoggerFactory.getLogger(getClass)

  private val config = ConfigFactory.load()
  private val brokers = config.getString("kafka.brokers").split(",")
  private val consumerConfig = UltiKafkaConsumerConfig(s"$aggregateName-query")
  private val environment = config.getString("app.environment")
  private val applicationId = consumerConfig.consumerGroup match {
    case name: String if name.contains(s"-$environment") ⇒
      s"${consumerConfig.consumerGroup}-$aggregateName"
    case _ ⇒
      s"${consumerConfig.consumerGroup}-$aggregateName-$environment"
  }

  private val clearStateOnStartup = config.getBoolean("kafka.streams.wipe-state-on-start")
  private val cacheHeapPercentage = config.getDouble("kafka.streams.cache-heap-percentage")
  private val totalMemory = Runtime.getRuntime.maxMemory()
  private val cacheMemory = (totalMemory * cacheHeapPercentage).longValue
  log.debug("Kafka streams cache memory being used is {} bytes", cacheMemory)
  private val standbyReplicas = config.getInt("kafka.streams.num-standby-replicas")
  private val streamsConfig = Map(
    ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed",
    StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG -> standbyReplicas.toString,
    StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG -> classOf[AggregateStreamsRocksDBConfig].getName,
    StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG -> cacheMemory.toString)

  private val consumer: KafkaByteStreamsConsumer = KafkaByteStreamsConsumer(brokers = brokers, applicationId = applicationId, consumerConfig = consumerConfig,
    kafkaConfig = streamsConfig, applicationServerConfig = applicationHostPort)

  private val aggProcessor = new EventProcessor[Agg, Event](aggregateName, readFormatting, writeFormatting, extractAggregateId, processEvent, metricsProvider)

  lazy val streams: KafkaStreams = consumer.streams

  private val eventDeserializationTimer: Timer = metricsProvider.createTimer(s"${aggregateName}EventDeserializationTimer")

  private def initStreams(): Unit = {
    consumer.builder.addStateStore(aggProcessor.aggregateKTableStoreBuilder)
    val events = consumer.stream(eventsTopic).flatMapValues { value ⇒
      eventDeserializationTimer.time(Try(readFormatting.readEvent(value)).toOption)
    }
    events.process(aggProcessor.supplier, aggProcessor.aggregateKTableStoreName)
  }

  def createTopology(): Topology = {
    initStreams()
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
  }

  val aggregateKTableStoreName: String = aggProcessor.aggregateKTableStoreName
  lazy val aggregateQueryableStateStore: KafkaStreamsKeyValueStore[String, Array[Byte]] = {
    val underlying = consumer.streams.store(aggregateKTableStoreName, QueryableStoreTypes.keyValueStore[String, Array[Byte]]())
    new KafkaStreamsKeyValueStore(underlying)
  }
}
