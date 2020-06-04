// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.util.Properties
import akka.pattern.pipe
import akka.actor.Props
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.streams.AggregateStateStoreKafkaStreamsImpl._
import com.ultimatesoftware.kafka.streams.HealthyActor.GetHealth
import com.ultimatesoftware.scala.core.kafka.{ KafkaTopic, UltiKafkaConsumerConfig }
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.internals.{ KTableImpl, KTableImplExtensions }
import org.apache.kafka.streams.scala.ByteArrayKeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import scala.concurrent.Future

private[streams] class AggregateStateStoreKafkaStreamsImpl[Agg >: Null](
    aggregateName: String,
    stateTopic: KafkaTopic,
    partitionTrackerProvider: KafkaStreamsPartitionTrackerProvider,
    kafkaStateMetadataHandler: KafkaPartitionMetadataHandler,
    aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) ⇒ Boolean,
    applicationHostPort: Option[String],
    override val settings: AggregateStateStoreKafkaStreamsImplSettings)
  extends KafkaStreamLifeCycleManagement[String, Array[Byte], KafkaByteStreamsConsumer, Array[Byte]] {

  import DefaultSerdes._
  import ImplicitConversions._
  import KTableImplExtensions._
  import context.dispatcher

  val aggregateStateStoreName = settings.storeName

  val healthCheckName = "aggregate-state-store"

  val topologyProps = new Properties()
  topologyProps.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE)

  val streamsConfig = baseStreamsConfig ++ Map(
    ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed",
    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG -> Integer.MAX_VALUE.toString,
    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> settings.commitInterval.toString,
    StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG -> settings.standByReplicas.toString,
    StreamsConfig.TOPOLOGY_OPTIMIZATION -> StreamsConfig.OPTIMIZE,
    StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG -> classOf[AggregateStreamsRocksDBConfig].getName)

  val validationProcessor = new ValidationProcessor[Array[Byte]](aggregateName, aggregateValidator)

  override def subscribeListeners(consumer: KafkaByteStreamsConsumer): Unit = {
    // In addition to the listener added by the KafkaStreamLifeCycleManagement we need to also subscribe this one
    val partitionTrackerListener = new KafkaStreamsUpdatePartitionsOnStateChangeListener(aggregateStateStoreName, partitionTrackerProvider.create(consumer.streams), false)
    consumer.streams.setStateListener(
      new KafkaStreamsStateChangeWithMultipleListeners(stateChangeListener, partitionTrackerListener))
    consumer.streams.setGlobalStateRestoreListener(stateRestoreListener)
    consumer.streams.setUncaughtExceptionHandler(uncaughtExceptionListener)
  }

  override def initialize(consumer: KafkaByteStreamsConsumer): Unit = {
    val aggregateStoreMaterialized = Materialized.as[String, Array[Byte], ByteArrayKeyValueStore](aggregateStateStoreName)
      .withValueSerde(new ByteArraySerde())

    // Build the KTable directly from Kafka
    val aggKTable = consumer.builder.table(stateTopic.name, aggregateStoreMaterialized)

    // Reach into the underlying implementation and tell it to send the old value for an aggregate
    // along with the newly updated value. The old value is only used for validation
    val aggKTableJavaImpl = aggKTable.inner.asInstanceOf[KTableImpl[String, Array[Byte], Array[Byte]]]
    aggKTableJavaImpl.sendOldValues()

    // Run business logic validation and transform values from an aggregate into metadata about
    // the processed record including topic/partition, and offset of the message in Kafka
    val stateMeta = aggKTableJavaImpl.toStreamWithChanges.transformValues(validationProcessor.supplier)
    kafkaStateMetadataHandler.processPartitionMetadata(stateMeta)
  }

  override def createQueryableStore(consumer: KafkaByteStreamsConsumer): KafkaStreamsKeyValueStore[String, Array[Byte]] = {
    val underlying = consumer.streams.store(aggregateStateStoreName, QueryableStoreTypes.keyValueStore[String, Array[Byte]]())
    new KafkaStreamsKeyValueStore(underlying)
  }

  override def createConsumer(): KafkaByteStreamsConsumer =
    KafkaByteStreamsConsumer(brokers = settings.brokers, applicationId = settings.applicationId, consumerConfig = settings.consumerConfig,
      kafkaConfig = streamsConfig, applicationServerConfig = applicationHostPort, topologyProps = Some(topologyProps))

  override def uninitialized: Receive = stashIt

  override def created(consumer: KafkaByteStreamsConsumer): Receive = stashIt

  override def running(
    consumer: KafkaByteStreamsConsumer,
    aggregateQueryableStateStore: KafkaStreamsKeyValueStore[String, Array[Byte]]): Receive = {
    case GetSubstatesForAggregate(aggregateId) ⇒
      getSubstatesForAggregate(aggregateQueryableStateStore, aggregateId).pipeTo(sender())
    case GetAggregateBytes(aggregateId) ⇒
      getAggregateBytes(aggregateQueryableStateStore, aggregateId).pipeTo(sender())
    case GetTopology ⇒
      sender() ! consumer.topology
    case GetHealth ⇒
      val status = if (consumer.streams.state().isRunning) HealthCheckStatus.UP else HealthCheckStatus.DOWN
      sender() ! getHealth(status)
  }

  def stashIt: Receive = {
    case _: GetAggregateBytes | _: GetSubstatesForAggregate | GetTopology ⇒
      stash()
  }

  def getSubstatesForAggregate(
    aggregateQueryableStateStore: KafkaStreamsKeyValueStore[String, Array[Byte]],
    aggregateId: String): Future[List[(String, Array[Byte])]] = {
    val unfiltered = aggregateQueryableStateStore.range(aggregateId, s"$aggregateId:~")
    unfiltered.map { result ⇒
      result.filter {
        case (key, _) ⇒
          val keyBeforeColon = key.takeWhile(_ != ':')
          keyBeforeColon == aggregateId
      }
    }
  }

  def getAggregateBytes(aggregateQueryableStateStore: KafkaStreamsKeyValueStore[String, Array[Byte]], aggregateId: String): Future[Option[Array[Byte]]] = {
    aggregateQueryableStateStore.get(aggregateId)
  }

}

private[streams] object AggregateStateStoreKafkaStreamsImpl {

  sealed trait AggregateStateStoreKafkaStreamsCommand
  case object GetTopology extends AggregateStateStoreKafkaStreamsCommand
  case class GetSubstatesForAggregate(aggregateId: String) extends AggregateStateStoreKafkaStreamsCommand
  case class GetAggregateBytes(aggregateId: String) extends AggregateStateStoreKafkaStreamsCommand

  def props(
    aggregateName: String,
    stateTopic: KafkaTopic,
    partitionTrackerProvider: KafkaStreamsPartitionTrackerProvider,
    kafkaStateMetadataHandler: KafkaPartitionMetadataHandler,
    aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) ⇒ Boolean,
    applicationHostPort: Option[String],
    settings: AggregateStateStoreKafkaStreamsImplSettings): Props = {
    Props(
      new AggregateStateStoreKafkaStreamsImpl(
        aggregateName,
        stateTopic,
        partitionTrackerProvider,
        kafkaStateMetadataHandler,
        aggregateValidator,
        applicationHostPort,
        settings))
  }

  case class AggregateStateStoreKafkaStreamsImplSettings(
      storeName: String,
      brokers: Seq[String],
      consumerConfig: UltiKafkaConsumerConfig,
      applicationId: String,
      cacheMemory: Long,
      standByReplicas: Int,
      commitInterval: Int,
      clearStateOnStartup: Boolean) extends KafkaStreamSettings

  object AggregateStateStoreKafkaStreamsImplSettings {
    def apply(consumerGroupName: String, aggregateName: String): AggregateStateStoreKafkaStreamsImplSettings = {
      val config = ConfigFactory.load()
      val aggregateStateStoreName: String = s"${aggregateName}AggregateStateStore"
      val brokers = config.getString("kafka.brokers").split(",")
      val consumerConfig = UltiKafkaConsumerConfig(consumerGroupName)
      val environment = config.getString("app.environment")
      val applicationId = consumerConfig.consumerGroup match {
        case name: String if name.contains(s"-$environment") ⇒
          s"${consumerConfig.consumerGroup}-$aggregateName"
        case _ ⇒
          s"${consumerConfig.consumerGroup}-$aggregateName-$environment"
      }
      val cacheHeapPercentage = config.getDouble("kafka.streams.cache-heap-percentage")
      val totalMemory = Runtime.getRuntime.maxMemory()
      val cacheMemory = (totalMemory * cacheHeapPercentage).longValue
      val standbyReplicas = config.getInt("kafka.streams.num-standby-replicas")
      val commitInterval = config.getInt("kafka.streams.commit-interval-ms")
      val clearStateOnStartup = config.getBoolean("kafka.streams.wipe-state-on-start")

      new AggregateStateStoreKafkaStreamsImplSettings(
        aggregateStateStoreName,
        brokers,
        consumerConfig,
        applicationId,
        cacheMemory,
        standbyReplicas,
        commitInterval,
        clearStateOnStartup)
    }
  }
}
