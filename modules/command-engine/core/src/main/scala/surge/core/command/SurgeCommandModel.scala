// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.command

import io.opentracing.Tracer
import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting, SurgeWriteFormatting }
import surge.internal.SurgeModel
import surge.internal.domain.AggregateProcessingModel
import surge.internal.kafka.SurgeKafkaConfig
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

private[surge] case class SurgeCommandKafkaConfig(
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    publishStateOnly: Boolean,
    streamsApplicationId: String,
    clientId: String,
    transactionalIdPrefix: String)
    extends SurgeKafkaConfig {
  override val eventsTopicOpt: Option[KafkaTopic] = if (publishStateOnly) None else Some(eventsTopic)
}

private[surge] case class SurgeCommandModel[Agg, Command, +Rej, Event](
    override val aggregateName: String,
    override val kafka: SurgeCommandKafkaConfig,
    override val model: AggregateProcessingModel[Agg, Command, Rej, Event],
    override val readFormatting: SurgeAggregateReadFormatting[Agg],
    writeFormatting: SurgeWriteFormatting[Agg, Event],
    override val aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean,
    override val metrics: Metrics,
    override val tracer: Tracer)
    extends SurgeModel[Agg, Command, Rej, Event] {
  override val aggregateWriteFormatting: SurgeAggregateWriteFormatting[Agg] = writeFormatting
  override val eventWriteFormatting: SurgeEventWriteFormatting[Event] = writeFormatting
}
