// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.commondsl

import surge.core.event.SurgeEventKafkaConfig

trait SurgeEventBusinessLogicTrait[AggId, Agg, Event] extends SurgeGenericBusinessLogicTrait[AggId, Agg, Nothing, Event] {
  def eventModel: SurgeProcessingModelCoreTrait[Agg, Nothing, Event]
  override def processingModel: SurgeProcessingModelCoreTrait[Agg, Nothing, Event] = eventModel

  def kafkaConfig: SurgeEventKafkaConfig = SurgeEventKafkaConfig(
    stateTopic = stateTopic,
    streamsApplicationId = streamsApplicationId,
    clientId = streamsClientId,
    transactionalIdPrefix = transactionalIdPrefix)
}
