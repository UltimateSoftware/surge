// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.commondsl

import surge.core.command.{ AggregateCommandModelCoreTrait, SurgeCommandKafkaConfig }
import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting, SurgeWriteFormatting }
import surge.kafka.KafkaTopic

trait SurgeGenericCommandBusinessLogicTrait[AggId, Agg, Command, Rej, Event] extends SurgeGenericBusinessLogicTrait[AggId, Agg, Command, Rej, Event] {

  def eventsTopic: KafkaTopic
  def writeFormatting: SurgeWriteFormatting[Agg, Event]
  def readFormatting: SurgeAggregateReadFormatting[Agg]

  override final def aggregateReadFormatting: SurgeAggregateReadFormatting[Agg] = readFormatting
  final def eventWriteFormatting: SurgeEventWriteFormatting[Event] = writeFormatting
  final override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[Agg] = writeFormatting

  def kafkaConfig: SurgeCommandKafkaConfig = new SurgeCommandKafkaConfig(
    stateTopic = stateTopic,
    eventsTopic = eventsTopic,
    streamsApplicationId = streamsApplicationId,
    clientId = streamsClientId,
    transactionalIdPrefix = transactionalIdPrefix,
    publishStateOnly = publishStateOnly)
}

trait SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event] extends SurgeGenericCommandBusinessLogicTrait[AggId, Agg, Command, Nothing, Event] {
  def commandModel: AggregateCommandModelCoreTrait[Agg, Command, Nothing, Event]
}

trait SurgeRejectableCommandBusinessLogicTrait[AggId, Agg, Command, Rej, Event] extends SurgeGenericCommandBusinessLogicTrait[AggId, Agg, Command, Rej, Event] {
  def commandModel: AggregateCommandModelCoreTrait[Agg, Command, Rej, Event]
}
