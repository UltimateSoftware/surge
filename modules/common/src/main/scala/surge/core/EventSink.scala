// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.concurrent.{ ExecutionContext, Future }

trait EventHandler[Event] {
  def eventHandler: Flow[Event, Any, NotUsed]
}

trait EventSink[Event] extends EventHandler[Event] {
  def parallelism: Int = 8
  def handleEvent(event: Event): Future[Any]
  def partitionBy(event: Event): String

  override def eventHandler: Flow[Event, Any, NotUsed] = {
    FlowConverter.flowFor(handleEvent, partitionBy, parallelism)(ExecutionContext.global)
  }
}
