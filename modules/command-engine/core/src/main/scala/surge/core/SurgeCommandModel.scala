// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import io.opentracing.Tracer
import surge.internal.domain.AggregateProcessingModel
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

private[surge] trait SurgeKafkaConfig {
  def stateTopic: KafkaTopic
  def eventsTopic: KafkaTopic
  def streamsApplicationId: String
  def clientId: String
  def transactionalIdPrefix: String
}
private[surge] case class SurgeCommandKafkaConfig(
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    publishStateOnly: Boolean,
    streamsApplicationId: String,
    clientId: String,
    transactionalIdPrefix: String)
    extends SurgeKafkaConfig

private[surge] case class SurgeCommandModel[Agg, Command, +Rej, Event](
    override val aggregateName: String,
    override val kafka: SurgeCommandKafkaConfig,
    model: AggregateProcessingModel[Agg, Command, Rej, Event],
    override val readFormatting: SurgeAggregateReadFormatting[Agg],
    override val writeFormatting: SurgeWriteFormatting[Agg, Event],
    override val aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean,
    override val metrics: Metrics,
    override val tracer: Tracer)
    extends SurgeModel[Agg, Event] {}
