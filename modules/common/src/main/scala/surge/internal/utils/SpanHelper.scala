// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import io.opentracing.{ Span, Tracer }

import scala.jdk.CollectionConverters._

trait SpanSupport {
  def tracer: Tracer

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

object SpanExtensions {
  implicit class SpanExtensions(span: Span) {
    def error(throwable: Throwable): Span = {
      span.setTag(io.opentracing.tag.Tags.ERROR, true: java.lang.Boolean)
      log(Map("event" -> "error", "message" -> throwable.getMessage, "stack" -> throwable.getStackTrace.mkString("\n")))
    }

    def log(fields: Map[String, AnyRef]): Span = {
      span.log(fields.asJava)
    }
  }
}

class SpanHelper(override val tracer: Tracer) extends SpanSupport
