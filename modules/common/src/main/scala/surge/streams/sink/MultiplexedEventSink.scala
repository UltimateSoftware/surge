// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.streams.sink

import surge.streams.EventSink

import scala.concurrent.{ ExecutionContext, Future }

trait EventSinkMultiplexAdapter[BaseEvent, SinkSubtype <: BaseEvent] {
  private[sink] type Subtype = SinkSubtype
  def convertEvent(event: BaseEvent): Option[SinkSubtype]
  def sink: EventSink[SinkSubtype]
}

trait MultiplexedEventSink[Event] extends EventSink[Event] {
  implicit def ec: ExecutionContext
  def destinationSinks: Seq[EventSinkMultiplexAdapter[Event, _ <: Event]]

  override def handleEvent(key: String, event: Event, headers: Map[String, Array[Byte]]): Future[Any] = {
    val futureHandled = destinationSinks.flatMap { adapter =>
      type Subtype = adapter.Subtype
      val typedAdapter: EventSinkMultiplexAdapter[Event, Subtype] = adapter.asInstanceOf[EventSinkMultiplexAdapter[Event, Subtype]]
      typedAdapter.convertEvent(event).map { evt =>
        typedAdapter.sink.handleEvent(key, evt, headers)
      }
    }
    Future.sequence(futureHandled)
  }
}
