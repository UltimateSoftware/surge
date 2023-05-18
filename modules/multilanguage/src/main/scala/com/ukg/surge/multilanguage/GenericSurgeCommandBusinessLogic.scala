// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import com.ukg.surge.multilanguage.protobuf.BusinessLogicService
import surge.core.commondsl.SurgeProcessingModelCoreTrait
import surge.core.{ SerializedAggregate, SerializedMessage, SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.kafka.KafkaTopic
import surge.scaladsl.command.SurgeCommandBusinessLogic

import java.util.UUID

class GenericSurgeCommandBusinessLogic(aggregName: String, eventsTopicName: String, stateTopicName: String, bridgeToBusinessApp: BusinessLogicService)(
    implicit system: ActorSystem)
    extends SurgeCommandBusinessLogic[UUID, SurgeState, SurgeCmd, SurgeEvent] {

  import Implicits._

  override def commandModel: SurgeProcessingModelCoreTrait[SurgeState, SurgeCmd, SurgeEvent] =
    new GenericAsyncAggregateCommandModel(bridgeToBusinessApp)

  override def eventsTopic: KafkaTopic = KafkaTopic(eventsTopicName)

  override def aggregateReadFormatting: SurgeAggregateReadFormatting[SurgeState] = (body: Array[Byte]) => {
    val pbState: protobuf.State = protobuf.State.parseFrom(body)
    Some(pbState)
  }

  override def eventWriteFormatting: SurgeEventWriteFormatting[SurgeEvent] = (evt: SurgeEvent) => {
    val pbEvent: protobuf.Event = evt
    SerializedMessage(evt.aggregateId, pbEvent.toByteArray)
  }

  override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[SurgeState] = (state: SurgeState) => {
    val pbState: protobuf.State = state
    SerializedAggregate(pbState.toByteArray)
  }

  override def aggregateName: String = aggregName

  override def stateTopic: KafkaTopic = KafkaTopic(stateTopicName)
}
