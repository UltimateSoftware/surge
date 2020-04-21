// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional

import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.streams.core.{ KafkaStreamsCommandKafkaConfig, SurgeAggregateReadFormatting, SurgeWriteFormatting }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher, NoOpMetricsProvider, NoOpsMetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.{ AggregateCommandModel, AggregateComposer }
import play.api.libs.json.JsValue

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._

abstract class KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {

  private val config = ConfigFactory.load()

  def aggregateName: String
  def stateTopic: KafkaTopic
  def eventsTopic: KafkaTopic
  def internalMetadataTopic: KafkaTopic
  def eventKeyExtractor(e: Event): String
  def stateKeyExtractor(json: JsValue): String

  def commandModel: AggregateCommandModel[AggId, Agg, Command, Event, CmdMeta, EvtMeta]

  def readFormatting: SurgeAggregateReadFormatting[AggId, Agg]
  def writeFormatting: SurgeWriteFormatting[AggId, Agg, Event, EvtMeta]

  def commandValidator: AsyncCommandValidator[Command, Agg]
  def aggregateComposer: AggregateComposer[AggId, Agg]

  def aggregateValidator(key: String, aggJson: Array[Byte], prevAggJsonOpt: Optional[Array[Byte]]): Boolean = true
  private def scalaAggregateValidator: (String, Array[Byte], Option[Array[Byte]]) ⇒ Boolean = { (key, agg, prevAgg) ⇒
    aggregateValidator(key, agg, prevAgg.asJava)
  }

  // Defaults to noops publishing (for now) and 30 second interval on metrics snapshots
  // These can be overridden in the derived applications
  def metricsPublisher: MetricsPublisher = NoOpsMetricsPublisher
  def metricsInterval: FiniteDuration = 30.seconds

  def metricsProvider: MetricsProvider = NoOpMetricsProvider

  private def kafkaConfig = KafkaStreamsCommandKafkaConfig(stateTopic = stateTopic, eventsTopic = eventsTopic,
    internalMetadataTopic = internalMetadataTopic, eventKeyExtractor = eventKeyExtractor, stateKeyExtractor = stateKeyExtractor)

  def aggregateConsumerGroupName: String = {
    val environment = config.getString("kafka.environment")
    s"$aggregateName-$environment-command"
  }

  def internalConsumerGroupName: String = {
    val environment = config.getString("kafka.environment")
    s"global-ktable-$environment-${internalMetadataTopic.name}"
  }

  private[javadsl] def toCore: com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {
    new com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
      aggregateName = aggregateName, kafka = kafkaConfig,
      model = commandModel, writeFormatting = writeFormatting, readFormatting = readFormatting,
      commandValidator = commandValidator, aggregateValidator = scalaAggregateValidator,
      metricsProvider = metricsProvider, metricsPublisher = metricsPublisher, metricsInterval = metricsInterval,
      aggregateComposer = aggregateComposer,
      aggregateConsumerGroupName = aggregateConsumerGroupName,
      internalConsumerGroupName = internalConsumerGroupName)
  }
}
