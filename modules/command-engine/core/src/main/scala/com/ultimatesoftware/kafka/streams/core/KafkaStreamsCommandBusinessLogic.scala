// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.scala.core.kafka.{ KafkaPartitioner, KafkaTopic, PartitionStringUpToColon }
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.AggregateCommandModel
import org.apache.kafka.common.header.Headers

import scala.concurrent.duration._

private[streams] case class KafkaStreamsCommandKafkaConfig[Evt](
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    eventKeyExtractor: Evt ⇒ String,
    eventHeadersExtractor: Evt ⇒ Headers)

private[streams] case class KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    aggregateName: String,
    kafka: KafkaStreamsCommandKafkaConfig[Event],
    model: AggregateCommandModel[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    readFormatting: SurgeAggregateReadFormatting[AggId, Agg],
    writeFormatting: SurgeWriteFormatting[AggId, Agg, Event, EvtMeta],
    commandValidator: AsyncCommandValidator[Command, Agg],
    aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) ⇒ Boolean,
    metricsProvider: MetricsProvider, metricsPublisher: MetricsPublisher, metricsInterval: FiniteDuration,
    consumerGroup: String,
    transactionalIdPrefix: String) {
  val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon
}
