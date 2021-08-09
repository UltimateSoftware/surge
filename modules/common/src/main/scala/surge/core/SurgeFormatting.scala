// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import scala.jdk.CollectionConverters.MapHasAsScala

case class SerializedMessage(key: String, value: Array[Byte], headers: Map[String, String])
case class SerializedAggregate(value: Array[Byte], headers: Map[String, String] = Map.empty)

object SerializedAggregate {
  def create(value: Array[Byte], headers: java.util.Map[String, String]): SerializedAggregate = {
    SerializedAggregate(value, headers.asScala.toMap)
  }
}

object SerializedMessage {
  def create(key: String, value: Array[Byte], headers: java.util.Map[String, String]): SerializedMessage = {
    SerializedMessage(key, value, headers.asScala.toMap)
  }
}

trait SurgeEventReadFormatting[Event] {
  def readEvent(bytes: Array[Byte]): Event
}

trait SurgeAggregateReadFormatting[Agg] {
  def readState(bytes: Array[Byte]): Option[Agg]
}

@deprecated("Encourages the use of an anti-pattern. Use SurgeEventReadFormatting and SurgeAggregateReadFormatting separately.", "0.5.4")
trait SurgeReadFormatting[Agg, Event] extends SurgeEventReadFormatting[Event] with SurgeAggregateReadFormatting[Agg]

trait SurgeEventWriteFormatting[Event] {
  def writeEvent(evt: Event): SerializedMessage
}

trait SurgeAggregateWriteFormatting[Agg] {
  def writeState(agg: Agg): SerializedAggregate
}

@deprecated("Encourages the use of an anti-pattern. Use SurgeAggregateWriteFormatting and SurgeEventWriteFormatting separately.", "0.5.4")
trait SurgeWriteFormatting[Agg, Event] extends SurgeEventWriteFormatting[Event] with SurgeAggregateWriteFormatting[Agg]

trait SurgeAggregateFormatting[Agg] extends SurgeAggregateReadFormatting[Agg] with SurgeAggregateWriteFormatting[Agg]
trait SurgeEventFormatting[Event] extends SurgeEventReadFormatting[Event] with SurgeEventWriteFormatting[Event]
