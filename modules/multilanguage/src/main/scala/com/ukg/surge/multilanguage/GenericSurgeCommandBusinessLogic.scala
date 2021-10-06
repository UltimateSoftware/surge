// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import com.ukg.surge.multilanguage.protobuf.BusinessLogicService
import surge.core.{ SerializedAggregate, SerializedMessage, SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.core.command.AggregateCommandModelCoreTrait
import surge.kafka.KafkaTopic
import surge.scaladsl.command.SurgeCommandBusinessLogic

import java.util.UUID

class GenericSurgeCommandBusinessLogic(aggregName: String, eventsTopicName: String, stateTopicName: String, bridgeToBusinessApp: BusinessLogicService)(
    implicit system: ActorSystem)
    extends SurgeCommandBusinessLogic[UUID, SurgeState, SurgeCmd, SurgeEvent] {

  import Implicits._

  override def commandModel: AggregateCommandModelCoreTrait[SurgeState, SurgeCmd, Nothing, SurgeEvent] =
    new GenericAsyncAggregateCommandModel(bridgeToBusinessApp)

  override def eventsTopic: KafkaTopic = KafkaTopic(eventsTopicName)

  override def aggregateReadFormatting: SurgeAggregateReadFormatting[SurgeState] = (bytes: Array[Byte]) => {
    val pbState: protobuf.State = protobuf.State.parseFrom(bytes)
    Some(pbState)
  }

  override def eventWriteFormatting: SurgeEventWriteFormatting[SurgeEvent] = (evt: SurgeEvent) => {
    val pbEvent: protobuf.Event = evt
    SerializedMessage(key = evt.aggregateId, value = pbEvent.toByteArray, headers = Map.empty)
  }

  override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[SurgeState] = (surgeState: SurgeState) => {
    val pbState: protobuf.State = surgeState
    new SerializedAggregate(pbState.toByteArray, Map.empty)
  }

  override def aggregateName: String = aggregName

  override def stateTopic: KafkaTopic = KafkaTopic(stateTopicName)
}
