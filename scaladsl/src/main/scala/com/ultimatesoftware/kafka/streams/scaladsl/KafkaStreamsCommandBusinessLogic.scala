// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import com.ultimatesoftware.kafka.streams.core.{ KafkaStreamsCommandKafkaConfig, SurgeAggregateReadFormatting, SurgeWriteFormatting }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher, NoOpsMetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.{ AggregateCommandModel, AggregateComposer }
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

  def readFormatting: SurgeAggregateReadFormatting[AggId, Agg]
  def writeFormatting: SurgeWriteFormatting[AggId, Agg, Event, EvtMeta]

  def commandValidator: AsyncCommandValidator[Command, Agg]
  def aggregateComposer: AggregateComposer[AggId, Agg]

  def aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) ⇒ Boolean = { (_, _, _) ⇒ true }

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
      model = commandModel, readFormatting = readFormatting,
      writeFormatting = writeFormatting, commandValidator = commandValidator, aggregateValidator = aggregateValidator,
      metricsProvider = metricsProvider, metricsPublisher = metricsPublisher, metricsInterval = metricsInterval,
      aggregateComposer = aggregateComposer)
  }
}
