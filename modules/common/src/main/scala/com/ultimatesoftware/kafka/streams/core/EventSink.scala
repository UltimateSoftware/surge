// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.concurrent.{ ExecutionContext, Future }

trait EventHandler[Event] {
  def eventHandler: Flow[Event, Any, NotUsed]
}

trait EventSink[Event] extends EventHandler[Event] {
  def parallelism: Int = 1
  def handleEvent(event: Event): Future[Any]

  override def eventHandler: Flow[Event, Any, NotUsed] = {
    FlowConverter.flowFor(handleEvent, parallelism)(ExecutionContext.global)
  }
}
