// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams

import java.util.Properties
import akka.actor.ActorSystem
import akka.kafka.{ AutoSubscription, ConsumerSettings, Subscriptions }
import com.typesafe.config.{ Config, ConfigFactory }
import io.opentelemetry.api.trace.Tracer
import org.apache.kafka.common.serialization.Deserializer
import surge.internal.akka.kafka.AkkaKafkaConsumer
import surge.internal.streams.{ KafkaOffsetManagementSubscriptionProvider, KafkaStreamManager, ManagedDataPipeline, ManualOffsetManagementSubscriptionProvider }
import surge.internal.tracing.NoopTracerFactory
import surge.kafka.KafkaTopic
import surge.metrics.Metrics
import surge.streams.replay.{ DefaultEventReplaySettings, EventReplaySettings, EventReplayStrategy, NoOpEventReplayStrategy }

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

trait DataSource[Key, Value] {
  private val defaultReplayStrategy = new NoOpEventReplayStrategy
  def replayStrategy: EventReplayStrategy = defaultReplayStrategy
  def replaySettings: EventReplaySettings = DefaultEventReplaySettings
}

trait KafkaDataSource[Key, Value] extends DataSource[Key, Value] {
  private val config: Config = ConfigFactory.load()
  private val defaultBrokers = config.getString("kafka.brokers")

  def kafkaBrokers: String = defaultBrokers
  def kafkaTopic: KafkaTopic
  def subscription: AutoSubscription = Subscriptions.topics(kafkaTopic.name)

  def actorSystem: ActorSystem

  def keyDeserializer: Deserializer[Key]
  def valueDeserializer: Deserializer[Value]

  def metrics: Metrics = Metrics.globalMetricRegistry

  def tracer: Tracer = NoopTracerFactory.create()

  def additionalKafkaProperties: Properties = new Properties()

  def offsetManager: OffsetManager = new DefaultKafkaOffsetManager

  def to(sink: DataHandler[Key, Value], consumerGroup: String): DataPipeline = {
    to(sink, consumerGroup, autoStart = true)
  }

  def to(sink: DataHandler[Key, Value], consumerGroup: String, autoStart: Boolean): DataPipeline = {
    val consumerSettings = AkkaKafkaConsumer
      .consumerSettings[Key, Value](actorSystem, groupId = consumerGroup, brokers = kafkaBrokers)(keyDeserializer, valueDeserializer)
      .withProperties(additionalKafkaProperties.asScala.toMap)
    to(consumerSettings)(sink, autoStart)
  }

  private def getStreamManager(consumerSettings: ConsumerSettings[Key, Value], sink: DataHandler[Key, Value])(
      implicit actorSystem: ActorSystem): KafkaStreamManager[Key, Value] = {
    val topicName = kafkaTopic.name
    val subscriptionProvider = offsetManager match {
      case _: DefaultKafkaOffsetManager =>
        new KafkaOffsetManagementSubscriptionProvider[Key, Value](topicName, subscription, consumerSettings, sink)
      case _ =>
        new ManualOffsetManagementSubscriptionProvider[Key, Value](topicName, subscription, consumerSettings, sink, offsetManager)
    }
    new KafkaStreamManager[Key, Value](
      topicName = topicName,
      consumerSettings = consumerSettings,
      subscriptionProvider = subscriptionProvider,
      keyDeserializer = keyDeserializer,
      valueDeserializer = valueDeserializer,
      replayStrategy = replayStrategy,
      replaySettings = replaySettings,
      tracer = tracer)
  }

  private[streams] def to(consumerSettings: ConsumerSettings[Key, Value])(sink: DataHandler[Key, Value], autoStart: Boolean): DataPipeline = {
    implicit val system: ActorSystem = actorSystem
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val pipeline = new ManagedDataPipeline(getStreamManager(consumerSettings, sink), metrics)
    if (autoStart) {
      pipeline.start()
    }
    pipeline
  }
}
