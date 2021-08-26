// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.event

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import surge.core.commondsl.SurgeEventBusinessLogicTrait
import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.internal.SurgeModel
import surge.internal.domain.AggregateProcessingModel
import surge.internal.kafka.SurgeKafkaConfig
import surge.kafka.{ KafkaPartitioner, KafkaTopic }
import surge.metrics.Metrics

private[surge] case class SurgeEventKafkaConfig(stateTopic: KafkaTopic, streamsApplicationId: String, clientId: String, transactionalIdPrefix: String)
    extends SurgeKafkaConfig {
  override val eventsTopicOpt: Option[KafkaTopic] = None
}

object SurgeEventServiceModel {
  def apply[AggId, Agg, Event, Response](
      businessLogic: SurgeEventBusinessLogicTrait[AggId, Agg, Event, Response]): SurgeEventServiceModel[Agg, Event, Response] = {
    new SurgeEventServiceModel[Agg, Event, Response](
      aggregateName = businessLogic.aggregateName,
      kafka = businessLogic.kafkaConfig,
      model = businessLogic.eventModel.toCore,
      aggregateWriteFormatting = businessLogic.aggregateWriteFormatting,
      aggregateReadFormatting = businessLogic.aggregateReadFormatting,
      metrics = businessLogic.metrics,
      openTelemetry = businessLogic.openTelemetry,
      tracer = businessLogic.tracer,
      partitioner = businessLogic.partitioner)
  }
}
private[surge] case class SurgeEventServiceModel[Agg, Event, Response](
    override val aggregateName: String,
    override val kafka: SurgeEventKafkaConfig,
    override val model: AggregateProcessingModel[Agg, Nothing, Nothing, Event, Response],
    override val aggregateReadFormatting: SurgeAggregateReadFormatting[Agg],
    override val aggregateWriteFormatting: SurgeAggregateWriteFormatting[Agg],
    override val metrics: Metrics,
    override val openTelemetry: OpenTelemetry,
    override val tracer: Tracer,
    override val partitioner: KafkaPartitioner[String])
    extends SurgeModel[Agg, Nothing, Nothing, Event, Response] {
  override def eventWriteFormattingOpt: Option[SurgeEventWriteFormatting[Event]] = None
}
