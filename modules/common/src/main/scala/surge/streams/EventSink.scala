// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams

import akka.NotUsed
import akka.stream.scaladsl.Flow
import io.opentelemetry.api.trace.Span
import surge.internal.akka.streams.FlowConverter
import surge.internal.streams.{DefaultDataSinkExceptionHandler, DefaultDataSinkExceptionHandlerWithSupport, MetaFormatter}
import surge.metrics.{Metrics, Rate}

import scala.concurrent.{ExecutionContext, Future}

case class EventPlusStreamMeta[Key, Value, +Meta](messageKey: Key, messageBody: Value, streamMeta: Meta, headers: Map[String, Array[Byte]], span: Span)

abstract class EventSinkExceptionHandler[Evt] extends DataSinkExceptionHandler[String, Evt]

trait EventHandler[Event] {
  def eventHandler[Meta]: Flow[EventPlusStreamMeta[String, Event, Meta], Meta, NotUsed]

  def nullEventFactory(key: String, headers: Map[String, Array[Byte]]): Option[Event] = None

  def createSinkExceptionHandler: DataSinkExceptionHandler[String, Event] = this match {
    case support: EventSinkSupport[Event] =>
      new DefaultDataSinkExceptionHandlerWithSupport[String, Event](support.metrics, support.metricTags,
        new MetaFormatter[String, Event] {
          override def formatMeta[Meta](epm: EventPlusStreamMeta[String, Event, Meta]): String = support.formatMetaForExceptionHandlerMessage.apply(epm)
        }
      )
    case _ =>
      new DefaultDataSinkExceptionHandler()
  }
}

trait EventSink[Event] extends EventHandler[Event] {

  def sinkName: String = this.getClass.getSimpleName
  def parallelism: Int = 8

  def handleEvent(key: String, event: Event, headers: Map[String, Array[Byte]]): Future[Any]
  def partitionBy(key: String, event: Event, headers: Map[String, Array[Byte]]): String

  override def eventHandler[Meta]: Flow[EventPlusStreamMeta[String, Event, Meta], Meta, NotUsed] = {
    FlowConverter.flowFor(sinkName, handleEvent, partitionBy, createsinkExceptionHandler, parallelism)(ExecutionContext.global)
  }
}

trait EventSinkSupport[Event] {
  def metrics: Metrics = Metrics.globalMetricRegistry
  def metricTags: Map[String, String] = Map("class" -> getClass.getCanonicalName)
  def formatMetaForExceptionHandlerMessage[Meta]: EventPlusStreamMeta[String, Event, Meta] => String = _.streamMeta.toString
}

abstract class AbstractEventSink[Event] extends EventSink[Event] with EventSinkSupport[Event]
