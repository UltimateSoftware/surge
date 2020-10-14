// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.concurrent.{ ExecutionContext, Future }

trait EventHandler[Event, EvtMeta] {
  def eventHandler: Flow[(Event, EvtMeta), Any, NotUsed]
}

trait EventSink[Event, EvtMeta] extends EventHandler[Event, EvtMeta] {
  def parallelism: Int = 1
  def handleEvent(event: Event, eventProps: EvtMeta): Future[Any]

  override def eventHandler: Flow[(Event, EvtMeta), Any, NotUsed] = {
    FlowConverter.flowFor(handleEvent, parallelism)(ExecutionContext.global)
  }
}
