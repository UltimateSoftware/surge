// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.event

import io.opentracing.Tracer
import surge.core.commondsl.AbstractSurgeEventBusinessLogic
import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.internal.SurgeModel
import surge.internal.domain.AggregateProcessingModel
import surge.internal.kafka.SurgeKafkaConfig
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

private[surge] case class SurgeEventKafkaConfig private (
    stateTopic: KafkaTopic,
    publishStateOnly: Boolean,
    streamsApplicationId: String,
    clientId: String,
    transactionalIdPrefix: String)
    extends SurgeKafkaConfig {
  override val eventsTopicOpt: Option[KafkaTopic] = None
}

object SurgeEventServiceModel {
  def apply[AggId, Agg, Event](businessLogic: AbstractSurgeEventBusinessLogic[AggId, Agg, Event]): SurgeEventServiceModel[Agg, Event] = {
    new SurgeEventServiceModel[Agg, Event](
      aggregateName = businessLogic.aggregateName,
      kafka = businessLogic.kafkaConfig,
      model = businessLogic.eventModel.toCore,
      aggregateWriteFormatting = businessLogic.aggregateFormatting,
      readFormatting = businessLogic.aggregateFormatting,
      eventWriteFormatting = businessLogic.eventWriteFormatting,
      aggregateValidator = businessLogic.aggregateValidatorLambda,
      metrics = businessLogic.metrics,
      tracer = businessLogic.tracer)
  }
}
private[surge] case class SurgeEventServiceModel[Agg, Event](
    override val aggregateName: String,
    override val kafka: SurgeEventKafkaConfig,
    override val model: AggregateProcessingModel[Agg, Nothing, Nothing, Event],
    override val readFormatting: SurgeAggregateReadFormatting[Agg],
    override val aggregateWriteFormatting: SurgeAggregateWriteFormatting[Agg],
    override val eventWriteFormatting: SurgeEventWriteFormatting[Event],
    override val aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean,
    override val metrics: Metrics,
    override val tracer: Tracer)
    extends SurgeModel[Agg, Nothing, Nothing, Event]
