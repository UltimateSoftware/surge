// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import surge.domain.AggregateCommandModel
import surge.kafka.{ KafkaPartitioner, KafkaTopic, PartitionStringUpToColon }
import surge.metrics.Metrics

private[surge] case class SurgeCommandKafkaConfig(
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    publishStateOnly: Boolean,
    consumerGroup: String,
    clientId: String,
    transactionalIdPrefix: String)

private[surge] case class SurgeCommandBusinessLogic[Agg, Command, Event](
    aggregateName: String,
    kafka: SurgeCommandKafkaConfig,
    model: AggregateCommandModel[Agg, Command, Event],
    readFormatting: SurgeAggregateReadFormatting[Agg],
    writeFormatting: SurgeWriteFormatting[Agg, Event],
    aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean,
    metrics: Metrics) {
  val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon
}
