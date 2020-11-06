// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

case class SerializedMessage(key: String, value: Array[Byte], headers: Map[String, String])
case class SerializedAggregate(value: Array[Byte], headers: Map[String, String])

trait SurgeEventReadFormatting[Event] {
  def readEvent(bytes: Array[Byte]): Event
}

trait SurgeAggregateReadFormatting[Agg] {
  def readState(bytes: Array[Byte]): Option[Agg]
}

trait SurgeReadFormatting[Agg, Event] extends SurgeEventReadFormatting[Event] with SurgeAggregateReadFormatting[Agg]

trait SurgeEventWriteFormatting[Event] {
  def writeEvent(evt: Event): SerializedMessage
}

trait SurgeAggregateWriteFormatting[Agg] {
  def writeState(agg: Agg): SerializedAggregate
}
trait SurgeWriteFormatting[Agg, Event] extends SurgeEventWriteFormatting[Event] with SurgeAggregateWriteFormatting[Agg]

trait SurgeAggregateFormatting[Agg] extends SurgeAggregateReadFormatting[Agg] with SurgeAggregateWriteFormatting[Agg]
trait SurgeEventFormatting[Event] extends SurgeEventReadFormatting[Event] with SurgeEventWriteFormatting[Event]
