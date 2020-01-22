// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.scala.oss.domain.AggregateSegment

trait SurgeEventReadFormatting[Event, EvtMeta] {
  def readEvent(bytes: Array[Byte]): (Event, Option[EvtMeta])
}

trait SurgeAggregateReadFormatting[AggId, Agg] {
  def readState(bytes: Array[Byte]): Option[AggregateSegment[AggId, Agg]]
}

trait SurgeReadFormatting[AggId, Agg, Event, EvtMeta] extends SurgeEventReadFormatting[Event, EvtMeta] with SurgeAggregateReadFormatting[AggId, Agg]

trait SurgeEventWriteFormatting[Event, EvtMeta] {
  def writeEvent(evt: Event, metadata: EvtMeta): Array[Byte]
}

trait SurgeAggregateWriteFormatting[AggId, Agg] {
  def writeState(agg: AggregateSegment[AggId, Agg]): Array[Byte]
}
trait SurgeWriteFormatting[AggId, Agg, Event, EvtMeta] extends SurgeEventWriteFormatting[Event, EvtMeta] with SurgeAggregateWriteFormatting[AggId, Agg]

trait SurgeAggregateFormatting[AggId, Agg] extends SurgeAggregateReadFormatting[AggId, Agg] with SurgeAggregateWriteFormatting[AggId, Agg]
trait SurgeEventFormatting[Event, EvtMeta] extends SurgeEventReadFormatting[Event, EvtMeta] with SurgeEventWriteFormatting[Event, EvtMeta]
