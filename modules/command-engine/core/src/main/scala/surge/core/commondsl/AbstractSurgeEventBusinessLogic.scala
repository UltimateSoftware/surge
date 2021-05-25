// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.commondsl

import com.typesafe.config.Config
import surge.core.event.{ AggregateEventModelCoreTrait, SurgeEventKafkaConfig }
import surge.internal.kafka.SurgeKafkaConfig
import surge.kafka.KafkaTopic

abstract class AbstractSurgeEventBusinessLogic[AggId, Agg, Event](final override protected[surge] val config: Config)
    extends SurgeGenericBusinessLogicTrait[AggId, Agg, Nothing, Nothing, Event] {

  protected[surge] def aggregateValidatorLambda: (String, Array[Byte], Option[Array[Byte]]) => Boolean

  def kafkaConfig: SurgeEventKafkaConfig = new SurgeEventKafkaConfig(
    stateTopic = stateTopic,
    streamsApplicationId = streamsApplicationId,
    clientId = streamsClientId,
    transactionalIdPrefix = transactionalIdPrefix)

  def eventModel: AggregateEventModelCoreTrait[Agg, Event]

}
