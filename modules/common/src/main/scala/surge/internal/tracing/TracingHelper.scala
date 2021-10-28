// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.tracing

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.{ Span, SpanBuilder, Tracer }

private[surge] trait SpanSupport {
  protected def tracer: Tracer

  /**
   * Creates a brand new span with no parent span
   * @param operationName
   *   The name for the created span
   * @return
   *   A new span with no configured parents
   */
  def newSpan(operationName: String): Span = {
    val span = tracer.spanBuilder(operationName).setNoParent().startSpan()
    span.makeCurrent()
    span
  }

  /**
   * Creates a new span whose parent automatically propagated from the current thread if one exists
   * @param operationName
   *   The name for the created span
   * @return
   *   A new span
   */
  def createSpan(operationName: String): Span = {
    val span = tracer.spanBuilder(operationName).startSpan()
    span.makeCurrent()
    span
  }

  /**
   * Creates a new span with an explicit parent
   * @param operationName
   *   The name for the created span
   * @param parentSpan
   *   The parent span for the newly created span
   * @return
   *   A new span with the explicitly provided parent
   */
  def childSpan(operationName: String, parentSpan: Span): Span = {
    val span = tracer.spanBuilder(operationName).setParent(io.opentelemetry.context.Context.root().`with`(parentSpan)).startSpan()
    span.makeCurrent()
    span
  }
}

trait TracingHelper {

  // alias for easy migration from OpenTracing
  implicit class TracerExt(tracer: Tracer) {
    def buildSpan(operationName: String): SpanBuilder = tracer.spanBuilder(operationName).setNoParent()
  }

  // alias for easy migration from OpenTracing
  implicit class SpanBuilderExt(spanBuilder: SpanBuilder) {
    def start(): Span = {
      val span = spanBuilder.startSpan()
      span.makeCurrent()
      span
    }
  }

  implicit class SpanExt(span: Span) {

    def error(throwable: Throwable): Span = {
      log("error", Map("message" -> throwable.getMessage))
    }

    // alias for easy migration from OpenTracing
    def log(eventName: String, fields: Map[String, String] = Map.empty): Span = {
      val attributesBuilder = Attributes.builder()
      fields.foreach { case (k, v) => attributesBuilder.put(k, v) }
      val attributes = attributesBuilder.build()
      span.addEvent(eventName, attributes)
    }

    // alias for easy migration from OpenTracing
    def finish(): Unit = span.end()

    // alias for easy migration from OpenTracing
    def setTag(key: String, value: String): Span = span.setAttribute(key, value)

  }
}

object TracingHelper extends TracingHelper
