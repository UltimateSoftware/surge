// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import surge.metrics.{ MetricsProvider, MetricsPublisher }
import surge.scala.core.kafka.{ KafkaPartitioner, KafkaTopic, PartitionStringUpToColon }
import surge.scala.core.validations.AsyncCommandValidator
import surge.scala.oss.domain.AggregateCommandModel

import scala.concurrent.duration._

private[surge] case class SurgeCommandKafkaConfig(
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    publishStateOnly: Boolean)

private[surge] case class SurgeCommandBusinessLogic[Agg, Command, Event](
    aggregateName: String,
    kafka: SurgeCommandKafkaConfig,
    model: AggregateCommandModel[Agg, Command, Event],
    readFormatting: SurgeAggregateReadFormatting[Agg],
    writeFormatting: SurgeWriteFormatting[Agg, Event],
    commandValidator: AsyncCommandValidator[Command, Agg],
    aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean,
    metricsProvider: MetricsProvider, metricsPublisher: MetricsPublisher, metricsInterval: FiniteDuration,
    consumerGroup: String,
    transactionalIdPrefix: String) {
  val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon
}
