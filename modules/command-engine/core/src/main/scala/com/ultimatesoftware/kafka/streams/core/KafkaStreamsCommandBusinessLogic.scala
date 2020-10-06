// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.scala.core.kafka.{ KafkaPartitioner, KafkaTopic, PartitionStringUpToColon }
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.AggregateCommandModel

import scala.concurrent.duration._

private[streams] case class KafkaStreamsCommandKafkaConfig(
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic)

private[streams] case class KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    aggregateName: String,
    kafka: KafkaStreamsCommandKafkaConfig,
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
