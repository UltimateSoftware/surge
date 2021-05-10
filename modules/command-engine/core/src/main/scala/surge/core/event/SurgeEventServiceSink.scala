// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.event

import surge.internal.utils.Logging
import surge.streams.EventSink

import scala.concurrent.{ ExecutionContext, Future }

trait SurgeEventServiceSink[AggId, Event] extends EventSink[Event] with Logging {

  implicit protected def executionContext: ExecutionContext
  protected def sendToAggregate(aggId: AggId, event: Event): Future[Any]

  def aggregateIdFromEvent: Event => AggId

  override def handleEvent(key: String, event: Event, headers: Map[String, Array[Byte]]): Future[Any] = {
    sendToAggregate(aggregateIdFromEvent(event), event)
  }
}
