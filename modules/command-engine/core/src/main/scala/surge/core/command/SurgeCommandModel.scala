// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.command

import io.opentracing.Tracer
import surge.core.commondsl.{ SurgeCommandBusinessLogicTrait, SurgeRejectableCommandBusinessLogicTrait }
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

private[surge] object SurgeCommandModel {
  def apply[AggId, Agg, Command, Event](
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event]): SurgeCommandModel[Agg, Command, Nothing, Event] = {
    new SurgeCommandModel[Agg, Command, Nothing, Event](
      aggregateName = businessLogic.aggregateName,
      kafka = businessLogic.kafkaConfig,
      model = businessLogic.commandModel.toCore,
      writeFormatting = businessLogic.writeFormatting,
      readFormatting = businessLogic.readFormatting,
      aggregateValidator = businessLogic.aggregateValidatorLambda,
      metrics = businessLogic.metrics,
      tracer = businessLogic.tracer)
  }
  def apply[AggId, Agg, Command, Rej, Event](
      businessLogic: SurgeRejectableCommandBusinessLogicTrait[AggId, Agg, Command, Rej, Event]): SurgeCommandModel[Agg, Command, Rej, Event] = {
    new SurgeCommandModel[Agg, Command, Rej, Event](
      aggregateName = businessLogic.aggregateName,
      kafka = businessLogic.kafkaConfig,
      model = businessLogic.commandModel.toCore,
      writeFormatting = businessLogic.writeFormatting,
      readFormatting = businessLogic.readFormatting,
      aggregateValidator = businessLogic.aggregateValidatorLambda,
      metrics = businessLogic.metrics,
      tracer = businessLogic.tracer)
  }

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
