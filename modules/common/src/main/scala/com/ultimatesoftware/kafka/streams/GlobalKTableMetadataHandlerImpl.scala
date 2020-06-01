// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import akka.actor.Props
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.streams.GlobalKTableMetadataHandlerImpl._
import com.ultimatesoftware.scala.core.kafka.{ JsonSerdes, KafkaTopic, UltiKafkaConsumerConfig }
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.{ QueryableStoreTypes, Stores }
import akka.pattern.pipe
import com.ultimatesoftware.support.inlineReceive
import org.apache.kafka.streams.KafkaStreams.State

private[streams] class GlobalKTableMetadataHandlerImpl(
    internalMetadataTopic: KafkaTopic,
    override val settings: GlobalKTableMetadataHandlerImplSettings)
  extends KafkaStreamLifeCycleManagement[String, String, KafkaStringStreamsConsumer, KafkaPartitionMetadata] {

  import DefaultSerdes._
  import ImplicitConversions._
  import context.dispatcher

  override val healthCheckName: String = "ktable-stream"

  implicit val partitionMetaSerde: Serde[KafkaPartitionMetadata] = JsonSerdes.serdeFor[KafkaPartitionMetadata]

  override val streamsConfig: Map[String, String] = baseStreamsConfig ++ Map[String, String](
    StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG -> classOf[KafkaPartitionMetadataGlobalStreamsRocksDBConfig].getName)

  override def createConsumer(): KafkaStringStreamsConsumer =
    KafkaStringStreamsConsumer(brokers = settings.brokers, applicationId = settings.applicationId, consumerConfig = settings.consumerConfig,
      kafkaConfig = streamsConfig, applicationServerConfig = None, topologyProps = None)

  override def createQueryableStore(consumer: KafkaStringStreamsConsumer): KafkaStreamsKeyValueStore[String, KafkaPartitionMetadata] = {
    val underlying = consumer.streams.store(settings.storeName, QueryableStoreTypes.keyValueStore[String, KafkaPartitionMetadata]())
    new KafkaStreamsKeyValueStore(underlying)
  }

  override def initialize(consumer: KafkaStringStreamsConsumer): Unit = {
    val storeSupplier = Stores.inMemoryKeyValueStore(settings.storeName)
    val stateMetaMaterializedStoreGlobal = Materialized.as[String, KafkaPartitionMetadata](storeSupplier)
      .withKeySerde(Serdes.String)
      .withValueSerde(JsonSerdes.serdeFor[KafkaPartitionMetadata])
    consumer.builder.globalTable(internalMetadataTopic.name, stateMetaMaterializedStoreGlobal)
  }

  // KafkaStreamLifeCycleManagement takes care of the rest of commands received on this stage
  override def uninitialized: Receive = stashIt orElse inlineReceive {
    case GetTopology ⇒
      stash()
    case IsOpen ⇒
      sender() ! false
  }

  override def created(consumer: KafkaStringStreamsConsumer): Receive = stashIt orElse inlineReceive {
    case GetTopology ⇒
      sender() ! consumer.topology
    case IsOpen ⇒
      sender() ! false
  }

  override def running(consumer: KafkaStringStreamsConsumer, stateStore: KafkaStreamsKeyValueStore[String, KafkaPartitionMetadata]): Receive = {
    case GetMeta(topicPartitionKey: String) ⇒
      stateStore.get(topicPartitionKey).pipeTo(sender())
    case IsOpen ⇒
      sender() ! (consumer.streams.state() == State.RUNNING)
    case GetTopology ⇒
      sender() ! consumer.topology
  }

  def stashIt: Receive = {
    case _: GetMeta ⇒
      stash()
  }
}

private[streams] object GlobalKTableMetadataHandlerImpl {

  sealed trait GlobalKTableMetadataHandlerCommand
  case object IsOpen extends GlobalKTableMetadataHandlerCommand
  case object GetTopology extends GlobalKTableMetadataHandlerCommand
  case class GetMeta(topicPartitionKey: String) extends GlobalKTableMetadataHandlerCommand

  def props(
    internalMetadataTopic: KafkaTopic,
    settings: GlobalKTableMetadataHandlerImplSettings): Props = {
    Props(
      new GlobalKTableMetadataHandlerImpl(
        internalMetadataTopic, settings))
  }

  case class GlobalKTableMetadataHandlerImplSettings(
      storeName: String,
      brokers: Seq[String],
      consumerConfig: UltiKafkaConsumerConfig,
      applicationId: String,
      cacheMemory: Long,
      clearStateOnStartup: Boolean) extends KafkaStreamSettings

  object GlobalKTableMetadataHandlerImplSettings {
    def apply(consumerGroupName: String, topicName: String): GlobalKTableMetadataHandlerImplSettings = {
      val config = ConfigFactory.load()
      val brokers = config.getString("kafka.brokers").split(",")
      val consumerConfig = UltiKafkaConsumerConfig(consumerGroupName)
      val applicationId = s"surge-aggregate-partitioner-progress.$topicName"
      val clearStateOnStartup = config.getBoolean("kafka.streams.wipe-state-on-start")
      val globalStateMetaStoreName: String = s"$topicName-global-table"
      val cacheMemory = 2 * 1024 * 1024L // 2Mb

      new GlobalKTableMetadataHandlerImplSettings(
        globalStateMetaStoreName,
        brokers,
        consumerConfig,
        applicationId,
        cacheMemory,
        clearStateOnStartup)
    }
  }
}
