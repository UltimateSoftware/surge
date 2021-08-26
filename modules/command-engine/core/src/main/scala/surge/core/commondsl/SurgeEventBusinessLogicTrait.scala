// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.commondsl

import surge.core.event.{ AggregateEventModelCoreTrait, SurgeEventKafkaConfig }

trait SurgeEventBusinessLogicTrait[AggId, Agg, Event] extends SurgeGenericBusinessLogicTrait[AggId, Agg, Nothing, Nothing, Event] {

  def kafkaConfig: SurgeEventKafkaConfig = new SurgeEventKafkaConfig(
    stateTopic = stateTopic,
    streamsApplicationId = streamsApplicationId,
    clientId = streamsClientId,
    transactionalIdPrefix = transactionalIdPrefix)

  def eventModel: AggregateEventModelCoreTrait[Agg, Event]

}
