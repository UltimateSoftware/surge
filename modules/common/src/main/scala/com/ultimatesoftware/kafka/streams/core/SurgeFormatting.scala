// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

case class SerializedMessage(key: String, value: Array[Byte], headers: Map[String, String])

trait SurgeEventReadFormatting[Event, EvtMeta] {
  def readEvent(bytes: Array[Byte]): (Event, Option[EvtMeta])
}

trait SurgeAggregateReadFormatting[AggId, Agg] {
  def readState(bytes: Array[Byte]): Option[Agg]
}

trait SurgeReadFormatting[AggId, Agg, Event, EvtMeta] extends SurgeEventReadFormatting[Event, EvtMeta] with SurgeAggregateReadFormatting[AggId, Agg]

trait SurgeEventWriteFormatting[Event, EvtMeta] {
  def writeEvent(evt: Event, metadata: EvtMeta): SerializedMessage
}

trait SurgeAggregateWriteFormatting[AggId, Agg] {
  def writeState(agg: Agg): Array[Byte]
}
trait SurgeWriteFormatting[AggId, Agg, Event, EvtMeta] extends SurgeEventWriteFormatting[Event, EvtMeta] with SurgeAggregateWriteFormatting[AggId, Agg]

trait SurgeAggregateFormatting[AggId, Agg] extends SurgeAggregateReadFormatting[AggId, Agg] with SurgeAggregateWriteFormatting[AggId, Agg]
trait SurgeEventFormatting[Event, EvtMeta] extends SurgeEventReadFormatting[Event, EvtMeta] with SurgeEventWriteFormatting[Event, EvtMeta]
