// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import io.opentracing.{ Span, Tracer }

import scala.jdk.CollectionConverters._

private[surge] trait SpanSupport {
  protected def tracer: Tracer

  def createSpan(operationName: String): Span = {
    createSpan(operationName, Option(tracer.activeSpan()))
  }

  def createSpan(operationName: String, parentSpan: Option[Span]): Span = {
    parentSpan.map(parent => childSpan(operationName, parent)).getOrElse(newSpan(operationName))
  }

  def newSpan(operationName: String): Span = {
    tracer.buildSpan(operationName).start()
  }

  def childSpan(operationName: String, parentSpan: Span): Span = {
    tracer.buildSpan(operationName).asChildOf(parentSpan).start()
  }
}

trait SpanExtensions {
  implicit class SpanExt(span: Span) {

    def error(throwable: Throwable): Span = {
      span.setTag(io.opentracing.tag.Tags.ERROR, true: java.lang.Boolean)
      log(Map("event" -> "error", "message" -> throwable.getMessage, "stack" -> throwable.getStackTrace.mkString("\n")))
    }

    /*
     * The purpose of this is to add a .log method on Span in order to make it more Scala friendly (i.e.
     * to be able to pass in a Scala map instead of a Java map).
     * @param eventName name of the event
     * @param fields fields as a Scala map
     * @return a span
     */
    def log(eventName: String, fields: Map[String, String] = Map.empty): Span = {
      val scalaMap = fields + ("event" -> eventName)
      import scala.jdk.CollectionConverters._
      span.log(scalaMap.asJava)
    }

    def log(fields: Map[String, AnyRef]): Span = {
      span.log(fields.asJava)
    }
  }
}

object SpanExtensions extends SpanExtensions
