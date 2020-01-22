// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.concurrent.CompletionStage

import com.ultimatesoftware.kafka.streams.core
import com.ultimatesoftware.scala.core.messaging.EventProperties

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

abstract class EventSink[Event] extends core.EventSink[Event, EventProperties] {
  override def handleEvent(event: Event, eventProps: EventProperties): Future[Any] = {
    eventHandler(event, eventProps).toScala
  }

  def eventHandler(event: Event, eventProps: EventProperties): CompletionStage[Object]
}
