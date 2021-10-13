// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import com.ukg.surge.multilanguage.protobuf.BusinessLogicService
import surge.core.{ SerializedAggregate, SerializedMessage, SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.core.command.AggregateCommandModelCoreTrait
import surge.kafka.KafkaTopic
import surge.scaladsl.command.SurgeCommandBusinessLogic
import surge.serialization.{ Deserializer, Serializer }

import java.util.UUID

class GenericSurgeCommandBusinessLogic(aggregName: String, eventsTopicName: String, stateTopicName: String, bridgeToBusinessApp: BusinessLogicService)(
    implicit system: ActorSystem)
    extends SurgeCommandBusinessLogic[UUID, SurgeState, SurgeCmd, SurgeEvent] {

  import Implicits._

  override def commandModel: AggregateCommandModelCoreTrait[SurgeState, SurgeCmd, Nothing, SurgeEvent] =
    new GenericAsyncAggregateCommandModel(bridgeToBusinessApp)

  override def eventsTopic: KafkaTopic = KafkaTopic(eventsTopicName)

  override def aggregateReadFormatting: SurgeAggregateReadFormatting[SurgeState] = new SurgeAggregateReadFormatting[SurgeState] {
    override def readState(bytes: Array[Byte]): Option[SurgeState] = {
      Some(stateDeserializer().deserialize(bytes))
    }

    override def stateDeserializer(): Deserializer[SurgeState] = (body: Array[Byte]) => {
      protobuf.State.parseFrom(body)
    }
  }

  override def eventWriteFormatting: SurgeEventWriteFormatting[SurgeEvent] = new SurgeEventWriteFormatting[SurgeEvent] {
    override def writeEvent(evt: SurgeEvent): SerializedMessage =
      SerializedMessage(key = evt.aggregateId, value = eventSerializer().serialize(evt), headers = Map.empty)

    override def eventSerializer(): Serializer[SurgeEvent] = (event: SurgeEvent) => {
      val pbEvent: protobuf.Event = event
      pbEvent.toByteArray
    }
  }

  override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[SurgeState] = new SurgeAggregateWriteFormatting[SurgeState] {
    override def writeState(agg: SurgeState): SerializedAggregate =
      SerializedAggregate(stateSerializer().serialize(agg), Map.empty)

    override def stateSerializer(): Serializer[SurgeState] = (state: SurgeState) => {
      val pbState: protobuf.State = state
      pbState.toByteArray
    }
  }

  override def aggregateName: String = aggregName

  override def stateTopic: KafkaTopic = KafkaTopic(stateTopicName)
}
