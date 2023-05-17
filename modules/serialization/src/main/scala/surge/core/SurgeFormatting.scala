// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.core

trait SurgeAggregateReadFormatting[State] {
  def readState(bytes: Array[Byte]): Option[State]
}

trait SurgeEventWriteFormatting[Event] {
  def writeEvent(evt: Event): SerializedMessage
}

trait SurgeAggregateWriteFormatting[STATE] {
  def writeState(state: STATE): SerializedAggregate
}

trait SurgeAggregateFormatting[State] extends SurgeAggregateReadFormatting[State] with SurgeAggregateWriteFormatting[State]
