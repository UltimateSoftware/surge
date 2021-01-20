// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.concurrent.{ ExecutionContext, Future }

case class EventPlusStreamMeta[Key, Value, Meta](messageKey: Key, messageBody: Value, streamMeta: Meta, headers: Map[String, Array[Byte]])

trait EventHandler[Event] {
  def eventHandler[Meta]: Flow[EventPlusStreamMeta[String, Event, Meta], Meta, NotUsed]
  def nullEventFactory(key: String, headers: Map[String, Array[Byte]]): Option[Event] = None
}

trait EventSink[Event] extends EventHandler[Event] {
  def parallelism: Int = 8
  def handleEvent(key: String, event: Event, headers: Map[String, Array[Byte]]): Future[Any]
  def partitionBy(key: String, event: Event, headers: Map[String, Array[Byte]]): String

  override def eventHandler[Meta]: Flow[EventPlusStreamMeta[String, Event, Meta], Meta, NotUsed] = {
    FlowConverter.flowFor(handleEvent, partitionBy, parallelism)(ExecutionContext.global)
  }
}
