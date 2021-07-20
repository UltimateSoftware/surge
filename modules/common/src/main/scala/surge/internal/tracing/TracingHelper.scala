// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.tracing

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.{ Span, SpanBuilder, Tracer }

import scala.jdk.CollectionConverters._

private[surge] trait SpanSupport {
  protected def tracer: Tracer

  def newSpan(operationName: String): Span = {
    tracer.spanBuilder(operationName).setNoParent().startSpan()
  }

  def createSpan(operationName: String): Span = newSpan(operationName)

  def childSpan(operationName: String, parentSpan: Span): Span = {
    tracer.spanBuilder(operationName).setParent(io.opentelemetry.context.Context.root().`with`(parentSpan)).startSpan()
  }
}

trait TracingHelper {

  implicit class TracerExt(tracer: Tracer) {
    def buildSpan(operationName: String): SpanBuilder = tracer.spanBuilder(operationName).setNoParent()
  }

  implicit class SpanBuilderExt(spanBuilder: SpanBuilder) {
    def start(): Span = spanBuilder.startSpan()
  }

  implicit class SpanExt(span: Span) {

    def error(throwable: Throwable): Span = {
      log("error", Map("message" -> throwable.getMessage))
    }

    /*
     * The purpose of this is to add a .log method on Span in order to make it more Scala friendly (i.e.
     * to be able to pass in a Scala map instead of a Java map).
     * @param eventName name of the event
     * @param fields fields as a Scala map
     * @return a span
     */
    def log(eventName: String, fields: Map[String, String] = Map.empty): Span = {
      val attributesBuilder = Attributes.builder()
      fields.foreach { case (k, v) => attributesBuilder.put(k, v) }
      val attributes = attributesBuilder.build()
      span.addEvent(eventName, attributes)
    }

    def finish(): Unit = span.end()

    def setTag(key: String, value: String): Span = span.setAttribute(key, value)

  }
}

object TracingHelper extends TracingHelper
