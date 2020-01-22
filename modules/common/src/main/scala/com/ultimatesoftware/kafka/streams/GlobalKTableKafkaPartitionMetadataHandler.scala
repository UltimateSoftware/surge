// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.util.UUID

import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.scala.core.kafka.{ JsonSerdes, KafkaStringProducer, KafkaTopic, UltiKafkaConsumerConfig }
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ByteArrayKeyValueStore
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

object GlobalStreamsWriteBufferSettings extends WriteBufferSettings {
  override def maxWriteBufferNumber: Int = 2
  override def writeBufferSizeMb: Int = 4
}

object GlobalStreamsBlockCacheSettings extends BlockCacheSettings {
  override def blockSizeKb: Int = 16
  override def blockCacheSizeMb: Int = 1
  override def cacheIndexAndFilterBlocks: Boolean = false
}

class KafkaPartitionMetadataGlobalStreamsRocksDBConfig extends CustomRocksDBConfigSetter(GlobalStreamsBlockCacheSettings, GlobalStreamsWriteBufferSettings)

class GlobalKTableMetadataHandler(internalMetadataTopic: KafkaTopic) extends KafkaPartitionMetadataHandler {
  import com.ultimatesoftware.kafka.streams.ImplicitConversions._

  private val config = ConfigFactory.load()
  private val brokers = config.getString("kafka.brokers").split(",")
  private val testMode = config.getBoolean("kafka.streams.test-mode")
  private val consumerGroupName = if (testMode) {
    // If running in test mode, use a different consumer group for each test instance so they all run in isolation
    s"global-ktable-${internalMetadataTopic.name}-test-${UUID.randomUUID()}"
  } else {
    s"global-ktable-${internalMetadataTopic.name}"
  }
  private val globalConsumerConfig = UltiKafkaConsumerConfig(consumerGroupName)
  private val twoMb = 2 * 1024 * 1024L
  private val globalStreamsConfig = Map[String, String](
    StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG -> twoMb.toString,
    StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG -> classOf[KafkaPartitionMetadataGlobalStreamsRocksDBConfig].getName)
  private val globalKTableConsumer = KafkaStringStreamsConsumer(brokers, globalConsumerConfig,
    globalStreamsConfig, applicationServerConfig = None, topologyProps = None)
  private val clearStateOnStartup = config.getBoolean("kafka.streams.wipe-state-on-start")

  private val globalStateMetaStoreName = s"${internalMetadataTopic.name}-global-table"

  private lazy val globalStreams = globalKTableConsumer.streams

  private implicit val stringSerde: Serde[String] = JsonSerdes.serdeFor[String]
  private implicit val aggregateSerde: Serde[KafkaPartitionMetadata] = JsonSerdes.serdeFor[KafkaPartitionMetadata]

  override def initialize(): Unit = {
    val stateMetaMaterializedStoreGlobal = Materialized.as[String, KafkaPartitionMetadata, ByteArrayKeyValueStore](globalStateMetaStoreName)
      .withValueSerde(JsonSerdes.serdeFor[KafkaPartitionMetadata])
    globalKTableConsumer.builder.globalTable(internalMetadataTopic.name, stateMetaMaterializedStoreGlobal)

    globalKTableConsumer.streams.setGlobalStateRestoreListener(new KafkaStreamsStateRestoreListener)
    globalKTableConsumer.streams.setUncaughtExceptionHandler(new KafkaStreamsUncaughtExceptionHandler)
  }

  private val log = LoggerFactory.getLogger(getClass)
  override def processPartitionMetadata(stream: KStream[String, KafkaPartitionMetadata]): Unit = {
    val producer = KafkaStringProducer(brokers, internalMetadataTopic)
    stream.mapValues { value ⇒
      val topicPartition = s""""${value.topic}:${value.partition}""""
      log.trace("Updating StateMeta for {} to {}", Seq(topicPartition, value): _*)
      producer.putKeyValue(topicPartition, Json.toJson(value).toString())
    }
  }

  override def start(): Unit = {
    if (clearStateOnStartup) {
      globalStreams.cleanUp()
    }
    globalStreams.start()
  }

  lazy val stateMetaQueryableStore: KafkaStreamsKeyValueStore[String, KafkaPartitionMetadata] = {
    val underlying = globalStreams.store(globalStateMetaStoreName, QueryableStoreTypes.keyValueStore[String, KafkaPartitionMetadata]())
    new KafkaStreamsKeyValueStore(underlying)
  }
}
