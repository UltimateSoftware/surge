// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.commondsl

import surge.core.command.{ AggregateCommandModelCoreTrait, SurgeCommandKafkaConfig }
import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.kafka.KafkaTopic

trait SurgeGenericCommandBusinessLogicTrait[AggId, Agg, Command, Rej, Event] extends SurgeGenericBusinessLogicTrait[AggId, Agg, Command, Rej, Event] {

  def eventsTopic: KafkaTopic

  def aggregateReadFormatting: SurgeAggregateReadFormatting[Agg]
  def eventWriteFormatting: SurgeEventWriteFormatting[Event]
  def aggregateWriteFormatting: SurgeAggregateWriteFormatting[Agg]

  def kafkaConfig: SurgeCommandKafkaConfig = SurgeCommandKafkaConfig(
    stateTopic = stateTopic,
    eventsTopic = eventsTopic,
    streamsApplicationId = streamsApplicationId,
    clientId = streamsClientId,
    transactionalIdPrefix = transactionalIdPrefix,
    publishStateOnly = publishStateOnly)
}

trait SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event, Response] extends SurgeGenericCommandBusinessLogicTrait[AggId, Agg, Command, Nothing, Event] {
  def commandModel: AggregateCommandModelCoreTrait[Agg, Command, Nothing, Event, Response]
}

trait SurgeRejectableCommandBusinessLogicTrait[AggId, Agg, Command, Rej, Event, Response]
    extends SurgeGenericCommandBusinessLogicTrait[AggId, Agg, Command, Rej, Event] {
  def commandModel: AggregateCommandModelCoreTrait[Agg, Command, Rej, Event, Response]
}
