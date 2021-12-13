// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.command

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import surge.core.commondsl.SurgeCommandBusinessLogicTrait
import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.internal.SurgeModel
import surge.internal.domain.SurgeProcessingModel
import surge.internal.kafka.SurgeKafkaConfig
import surge.kafka.{ KafkaPartitioner, KafkaTopic }
import surge.metrics.Metrics

private[surge] case class SurgeCommandKafkaConfig(
    override val stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    publishStateOnly: Boolean,
    override val streamsApplicationId: String,
    override val clientId: String,
    override val transactionalIdPrefix: String)
    extends SurgeKafkaConfig {
  override val eventsTopicOpt: Option[KafkaTopic] = if (publishStateOnly) None else Some(eventsTopic)
}

private[surge] object SurgeCommandModel {
  def apply[AggId, Agg, Command, Event](businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event]): SurgeCommandModel[Agg, Command, Event] = {
    new SurgeCommandModel[Agg, Command, Event](
      aggregateName = businessLogic.aggregateName,
      kafka = businessLogic.kafkaConfig,
      model = businessLogic.processingModel.toCore,
      aggregateWriteFormatting = businessLogic.aggregateWriteFormatting,
      aggregateReadFormatting = businessLogic.aggregateReadFormatting,
      eventWriteFormatting = businessLogic.eventWriteFormatting,
      metrics = businessLogic.metrics,
      openTelemetry = businessLogic.openTelemetry,
      tracer = businessLogic.tracer,
      partitioner = businessLogic.partitioner)
  }
}
private[surge] case class SurgeCommandModel[Agg, Command, Event](
    override val aggregateName: String,
    override val kafka: SurgeCommandKafkaConfig,
    override val model: SurgeProcessingModel[Agg, Command, Event],
    override val aggregateWriteFormatting: SurgeAggregateWriteFormatting[Agg],
    override val metrics: Metrics,
    override val openTelemetry: OpenTelemetry,
    override val tracer: Tracer,
    override val partitioner: KafkaPartitioner[String],
    override val aggregateReadFormatting: SurgeAggregateReadFormatting[Agg],
    eventWriteFormatting: SurgeEventWriteFormatting[Event])
    extends SurgeModel[Agg, Command, Event] {
  override val eventWriteFormattingOpt: Option[SurgeEventWriteFormatting[Event]] = Some(eventWriteFormatting)
}
