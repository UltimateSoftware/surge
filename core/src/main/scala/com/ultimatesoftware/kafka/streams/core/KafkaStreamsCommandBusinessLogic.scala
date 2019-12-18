// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.mp.serialization.envelope.Envelope
import com.ultimatesoftware.scala.core.kafka.{ KafkaPartitioner, KafkaTopic, PartitionStringUpToColon }
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.{ AggregateCommandModel, AggregateComposer }
import play.api.libs.json.JsValue

import scala.concurrent.duration._

private[streams] case class KafkaStreamsCommandKafkaConfig[Evt](
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    internalMetadataTopic: KafkaTopic,
    eventKeyExtractor: Evt ⇒ String,
    stateKeyExtractor: JsValue ⇒ String)

private[streams] case class KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope <: com.ultimatesoftware.mp.serialization.envelope.Envelope](
    aggregateName: String,
    kafka: KafkaStreamsCommandKafkaConfig[Event],
    model: AggregateCommandModel[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    readFormatting: SurgeReadFormatting[Agg, Event, Envelope],
    writeFormatting: SurgeWriteFormatting[AggId, Agg, Event, EvtMeta],
    commandValidator: AsyncCommandValidator[Command, Agg],
    aggregateValidator: (String, JsValue, Option[JsValue]) ⇒ Boolean,
    aggregateComposer: AggregateComposer[AggId, Agg],
    metricsProvider: MetricsProvider, metricsPublisher: MetricsPublisher, metricsInterval: FiniteDuration) {
  val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon
}
