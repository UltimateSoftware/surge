// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.stream.scaladsl.Flow

import scala.concurrent.{ ExecutionContext, Future }

case class EventPlusOffset[T](messageBody: T, committableOffset: CommittableOffset)

trait EventHandler[Event] {
  def eventHandler: Flow[EventPlusOffset[Event], CommittableOffset, NotUsed]
}

trait EventSink[Event] extends EventHandler[Event] {
  def parallelism: Int = 8
  def handleEvent(event: Event): Future[Any]
  def partitionBy(event: Event): String

  override def eventHandler: Flow[EventPlusOffset[Event], CommittableOffset, NotUsed] = {
    FlowConverter.flowFor(handleEvent, partitionBy, parallelism)(ExecutionContext.global)
  }
}
