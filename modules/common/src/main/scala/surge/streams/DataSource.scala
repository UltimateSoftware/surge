// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.streams

import java.util.Properties

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.common.serialization.Deserializer
import surge.internal.akka.kafka.AkkaKafkaConsumer
import surge.internal.streams.{ KafkaStreamManager, ManagedDataPipeline }
import surge.kafka.KafkaTopic
import surge.metrics.Metrics
import surge.streams.replay.{ DefaultEventReplaySettings, EventReplaySettings, EventReplayStrategy, NoOpEventReplayStrategy }

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

trait DataSource {
  def replayStrategy: EventReplayStrategy = NoOpEventReplayStrategy
  def replaySettings: EventReplaySettings = DefaultEventReplaySettings
}

trait KafkaDataSource[Key, Value] extends DataSource {
  private val config: Config = ConfigFactory.load()
  private val defaultBrokers = config.getString("kafka.brokers")

  def kafkaBrokers: String = defaultBrokers
  def kafkaTopic: KafkaTopic

  def actorSystem: ActorSystem

  def keyDeserializer: Deserializer[Key]
  def valueDeserializer: Deserializer[Value]

  def metrics: Metrics = Metrics.globalMetricRegistry

  def additionalKafkaProperties: Properties = new Properties()

  def to(sink: DataHandler[Key, Value], consumerGroup: String): DataPipeline = {
    to(sink, consumerGroup, autoStart = true)
  }

  def to(sink: DataHandler[Key, Value], consumerGroup: String, autoStart: Boolean): DataPipeline = {
    val consumerSettings = AkkaKafkaConsumer.consumerSettings[Key, Value](actorSystem, groupId = consumerGroup,
      brokers = kafkaBrokers)(keyDeserializer, valueDeserializer)
      .withProperties(additionalKafkaProperties.asScala.toMap)
    to(consumerSettings)(sink, autoStart)
  }

  private[streams] def to(consumerSettings: ConsumerSettings[Key, Value])(sink: DataHandler[Key, Value], autoStart: Boolean): DataPipeline = {
    implicit val system: ActorSystem = actorSystem
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val pipeline = new ManagedDataPipeline(KafkaStreamManager(kafkaTopic, consumerSettings, replayStrategy, replaySettings, sink.dataHandler), metrics)
    if (autoStart) {
      pipeline.start()
    }
    pipeline
  }
}
