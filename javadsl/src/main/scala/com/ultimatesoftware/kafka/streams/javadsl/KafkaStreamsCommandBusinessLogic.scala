// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional

import com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandKafkaConfig
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher, NoOpsMetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.{ AggregateCommandModel, AggregateComposer }
import play.api.libs.json.JsValue

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._

abstract class KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {
  def aggregateName: String

  def stateTopic: KafkaTopic
  def eventsTopic: KafkaTopic
  def internalMetadataTopic: KafkaTopic
  def eventKeyExtractor(e: Event): String
  def stateKeyExtractor(json: JsValue): String

  def commandModel: AggregateCommandModel[AggId, Agg, Command, Event, CmdMeta, EvtMeta]

  def commandValidator: AsyncCommandValidator[Command, Agg]
  def aggregateComposer: AggregateComposer[AggId, Agg]

  def aggregateValidator(key: String, aggJson: JsValue, prevAggJsonOpt: Optional[JsValue]): Boolean = true
  private def scalaAggregateValidator: (String, JsValue, Option[JsValue]) ⇒ Boolean = { (key, agg, prevAgg) ⇒
    aggregateValidator(key, agg, prevAgg.asJava)
  }

  // Defaults to noops publishing (for now) and 30 second interval on metrics snapshots
  // These can be overridden in the derived applications
  def metricsPublisher: MetricsPublisher = NoOpsMetricsPublisher
  def metricsInterval: FiniteDuration = 30.seconds

  def metricsProvider: MetricsProvider

  private def kafkaConfig = KafkaStreamsCommandKafkaConfig(stateTopic = stateTopic, eventsTopic = eventsTopic,
    internalMetadataTopic = internalMetadataTopic, eventKeyExtractor = eventKeyExtractor, stateKeyExtractor = stateKeyExtractor)

  private[javadsl] def toCore: com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {
    new com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
      aggregateName = aggregateName, kafka = kafkaConfig,
      model = commandModel, formatting = new JacksonEventFormatter, commandValidator = commandValidator, aggregateValidator = scalaAggregateValidator,
      metricsProvider = metricsProvider, metricsPublisher = metricsPublisher, metricsInterval = metricsInterval,
      aggregateComposer = aggregateComposer)
  }
}
