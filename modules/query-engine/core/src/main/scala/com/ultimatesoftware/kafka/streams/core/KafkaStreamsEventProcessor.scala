// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.streams.{ AggregateStreamsRocksDBConfig, KafkaByteStreamsConsumer, KafkaStreamsKeyValueStore }
import com.ultimatesoftware.scala.core.domain.{ StateMessage, StateTypeInfo }
import com.ultimatesoftware.scala.core.kafka.{ KafkaTopic, UltiKafkaConsumerConfig }
import com.ultimatesoftware.scala.core.messaging.EventProperties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.slf4j.LoggerFactory
import play.api.libs.json.Format

class KafkaStreamsEventProcessor[AggId, Agg, Event, EvtMeta <: EventProperties](
    aggregateName: String,
    aggregateTypeInfo: StateTypeInfo,
    readFormatting: SurgeReadFormatting[AggId, StateMessage[Agg], Event, EvtMeta],
    writeFormatting: SurgeAggregateWriteFormatting[AggId, StateMessage[Agg]],
    eventsTopic: KafkaTopic,
    applicationHostPort: Option[String],
    processEvent: (Option[Agg], Event, EventProperties) ⇒ Option[Agg])(implicit aggFormat: Format[Agg]) {

  private val log = LoggerFactory.getLogger(getClass)

  private val config = ConfigFactory.load()
  private val brokers = config.getString("kafka.brokers").split(",")
  private val consumerConfig = UltiKafkaConsumerConfig(s"$aggregateName-query")

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

  private val consumer: KafkaByteStreamsConsumer = KafkaByteStreamsConsumer(brokers, consumerConfig,
    kafkaConfig = streamsConfig,
    applicationServerConfig = applicationHostPort)

  private val envelopeUtils = new EnvelopeUtils(readFormatting)
  private val aggProcessor = new EventProcessor[AggId, Agg, Event, EvtMeta](aggregateTypeInfo, readFormatting, writeFormatting, processEvent)

  lazy val streams: KafkaStreams = consumer.streams

  private def initStreams(): Unit = {
    consumer.builder.addStateStore(aggProcessor.aggregateKTableStoreBuilder)
    val events = consumer.stream(eventsTopic).flatMapValues { value ⇒
      envelopeUtils.eventFromBytes(value)
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
