// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.event

import io.opentelemetry.api.trace.Tracer
import surge.core.commondsl.SurgeEventBusinessLogicTrait
import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.internal.SurgeModel
import surge.internal.domain.AggregateProcessingModel
import surge.internal.kafka.SurgeKafkaConfig
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

private[surge] case class SurgeEventKafkaConfig(stateTopic: KafkaTopic, streamsApplicationId: String, clientId: String, transactionalIdPrefix: String)
    extends SurgeKafkaConfig {
  override val eventsTopicOpt: Option[KafkaTopic] = None
}

object SurgeEventServiceModel {
  def apply[AggId, Agg, Event](businessLogic: SurgeEventBusinessLogicTrait[AggId, Agg, Event]): SurgeEventServiceModel[Agg, Event] = {
    new SurgeEventServiceModel[Agg, Event](
      aggregateName = businessLogic.aggregateName,
      kafka = businessLogic.kafkaConfig,
      model = businessLogic.eventModel.toCore,
      aggregateWriteFormatting = businessLogic.aggregateWriteFormatting,
      aggregateReadFormatting = businessLogic.aggregateReadFormatting,
      aggregateValidator = businessLogic.aggregateValidatorLambda,
      metrics = businessLogic.metrics,
      tracer = businessLogic.tracer)
  }
}
private[surge] case class SurgeEventServiceModel[Agg, Event](
    override val aggregateName: String,
    override val kafka: SurgeEventKafkaConfig,
    override val model: AggregateProcessingModel[Agg, Nothing, Nothing, Event],
    override val aggregateReadFormatting: SurgeAggregateReadFormatting[Agg],
    override val aggregateWriteFormatting: SurgeAggregateWriteFormatting[Agg],
    override val aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean,
    override val metrics: Metrics,
    override val tracer: Tracer)
    extends SurgeModel[Agg, Nothing, Nothing, Event] {
  override def eventWriteFormattingOpt: Option[SurgeEventWriteFormatting[Event]] = None
}
