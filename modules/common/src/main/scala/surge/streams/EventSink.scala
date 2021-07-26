// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams

import akka.NotUsed
import akka.stream.scaladsl.Flow
import io.opentelemetry.api.trace.Tracer
import surge.internal.akka.streams.FlowConverter
import surge.internal.streams.DefaultDataSinkExceptionHandler

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class EventPlusStreamMeta[Key, Value, Meta](messageKey: Key, messageBody: Value, streamMeta: Meta, headers: Map[String, Array[Byte]])

abstract class EventSinkExceptionHandler[Evt] extends DataSinkExceptionHandler[String, Evt]

trait EventHandler[Event] {
  def eventHandler[Meta]: Flow[EventPlusStreamMeta[String, Event, Meta], Meta, NotUsed]
  def nullEventFactory(key: String, headers: Map[String, Array[Byte]]): Option[Event] = None
  def sinkExceptionHandler: DataSinkExceptionHandler[String, Event] = new DefaultDataSinkExceptionHandler[String, Event]
}

trait EventSink[Event] extends EventHandler[Event] {
  val tracer: Tracer
  def sinkName: String = this.getClass.getSimpleName
  def parallelism: Int = 8
  def handleEvent(key: String, event: Event, headers: Map[String, Array[Byte]]): Future[Any]
  def partitionBy(key: String, event: Event, headers: Map[String, Array[Byte]]): String

  // execute the handleEvent function with tracing
  private def execHandleEvent(key: String, event: Event, headers: Map[String, Array[Byte]]): Future[Any] = {
    import surge.internal.tracing.TracingHelper._
    val operationName = s"$sinkName:${event.getClass.getSimpleName}"
    val span = tracer.spanBuilder(operationName).setNoParent().startSpan()
    val handleEventFut: Future[Any] = handleEvent(key, event, headers)
    handleEventFut.transform {
      case failure @ Failure(exception) =>
        span.error(exception)
        span.end()
        failure
      case success @ Success(_) =>
        span.end()
        success
    }(ExecutionContext.global)
  }
  override def eventHandler[Meta]: Flow[EventPlusStreamMeta[String, Event, Meta], Meta, NotUsed] = {
    FlowConverter.flowFor(execHandleEvent, partitionBy, sinkExceptionHandler, parallelism)(ExecutionContext.global)
  }
}
