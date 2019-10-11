// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.scaladsl

import com.ultimatesoftware.kafka.streams.core.{ KafkaStreamsCommandKafkaConfig, SurgeFormatting }
import com.ultimatesoftware.scala.core.domain._
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher, NoOpsMetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import play.api.libs.json.JsValue

import scala.concurrent.duration._

trait KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {
  def aggregateName: String

  def stateTopic: KafkaTopic
  def eventsTopic: KafkaTopic
  def internalMetadataTopic: KafkaTopic
  def eventKeyExtractor: Event ⇒ String
  def stateKeyExtractor: JsValue ⇒ String

  def commandModel: AggregateCommandModel[AggId, Agg, Command, Event, CmdMeta, EvtMeta]

  def formatting: SurgeFormatting[Command, Event]
  def commandValidator: AsyncCommandValidator[Command, Agg]
  def aggregateComposer: AggregateComposer[AggId, Agg]

  def aggregateValidator: (String, JsValue, Option[JsValue]) ⇒ Boolean = { (_, _, _) ⇒ true }

  // Defaults to noops publishing (for now) and 30 second interval on metrics snapshots
  // These can be overridden in the derived applications
  def metricsPublisher: MetricsPublisher = NoOpsMetricsPublisher
  def metricsInterval: FiniteDuration = 30.seconds

  def metricsProvider: MetricsProvider

  private def kafkaConfig = KafkaStreamsCommandKafkaConfig(stateTopic = stateTopic, eventsTopic = eventsTopic,
    internalMetadataTopic = internalMetadataTopic, eventKeyExtractor = eventKeyExtractor, stateKeyExtractor = stateKeyExtractor)

  private[scaladsl] def toCore: com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {
    new com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
      aggregateName = aggregateName, kafka = kafkaConfig,
      model = commandModel, formatting = formatting, commandValidator = commandValidator, aggregateValidator = aggregateValidator,
      metricsProvider = metricsProvider, metricsPublisher = metricsPublisher, metricsInterval = metricsInterval,
      aggregateComposer = aggregateComposer)
  }
}
