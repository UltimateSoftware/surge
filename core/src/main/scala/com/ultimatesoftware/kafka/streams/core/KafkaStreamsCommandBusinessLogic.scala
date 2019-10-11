// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.scala.core.domain._
import com.ultimatesoftware.scala.core.kafka.{ KafkaPartitioner, KafkaTopic, PartitionStringUpToColon }
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import play.api.libs.json.JsValue

import scala.concurrent.duration._

private[streams] case class KafkaStreamsCommandKafkaConfig[Evt](
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    internalMetadataTopic: KafkaTopic,
    eventKeyExtractor: Evt ⇒ String,
    stateKeyExtractor: JsValue ⇒ String)

private[streams] case class KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    aggregateName: String,
    kafka: KafkaStreamsCommandKafkaConfig[Event],
    model: AggregateCommandModel[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    formatting: SurgeFormatting[Command, Event],
    commandValidator: AsyncCommandValidator[Command, Agg],
    aggregateValidator: (String, JsValue, Option[JsValue]) ⇒ Boolean,
    aggregateComposer: AggregateComposer[AggId, Agg],
    metricsProvider: MetricsProvider, metricsPublisher: MetricsPublisher, metricsInterval: FiniteDuration) {
  val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon
}
