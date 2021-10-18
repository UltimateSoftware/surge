// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import surge.serialization.{ Deserializer, Serializer }

trait SurgeEventReadFormatting[Event] {
  def readEvent(bytes: Array[Byte]): Event = {
    eventDeserializer().deserialize(bytes)
  }

  def eventDeserializer(): Deserializer[Event]
}

trait SurgeAggregateReadFormatting[State] {
  def readState(bytes: Array[Byte]): Option[State] = {
    Option(stateDeserializer().deserialize(bytes))
  }

  def stateDeserializer(): Deserializer[State]
}

trait SurgeEventWriteFormatting[Event] {
  def writeEvent(evt: Event): SerializedMessage = {
    eventSerializer().serialize(evt).asSerializedMessage(key(evt))
  }

  def eventSerializer(): Serializer[Event]
  def key(evt: Event): String
}

trait SurgeAggregateWriteFormatting[STATE] {
  def writeState(state: STATE): SerializedAggregate
  def stateSerializer(): Serializer[STATE]
}

trait SurgeAggregateFormatting[State] extends SurgeAggregateReadFormatting[State] with SurgeAggregateWriteFormatting[State]
trait SurgeEventFormatting[Event] extends SurgeEventReadFormatting[Event] with SurgeEventWriteFormatting[Event]
