// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.commondsl

import surge.core.SurgeEventWriteFormatting
import surge.core.command.SurgeCommandKafkaConfig
import surge.kafka.KafkaTopic

trait SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event] extends SurgeGenericBusinessLogicTrait[AggId, Agg, Command, Event] {
  def commandModel: SurgeProcessingModelCoreTrait[Agg, Command, Event]
  override def processingModel: SurgeProcessingModelCoreTrait[Agg, Command, Event] = commandModel

  def eventsTopic: KafkaTopic

  def eventWriteFormatting: SurgeEventWriteFormatting[Event]

  def kafkaConfig: SurgeCommandKafkaConfig = SurgeCommandKafkaConfig(
    stateTopic = stateTopic,
    eventsTopic = eventsTopic,
    streamsApplicationId = streamsApplicationId,
    clientId = streamsClientId,
    transactionalIdPrefix = transactionalIdPrefix,
    publishStateOnly = publishStateOnly)
}
