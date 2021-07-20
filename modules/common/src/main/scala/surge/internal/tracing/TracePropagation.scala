// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.tracing

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.{ Span, Tracer }
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.{ ContextPropagators, TextMapGetter, TextMapSetter }

import scala.jdk.CollectionConverters._
import scala.collection.mutable

object TracePropagation {
  // https://opentelemetry.io/docs/java/manual_instrumentation/

  private val openTelemetry: OpenTelemetry =
    OpenTelemetry.propagating(ContextPropagators.create(io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator.getInstance()))

  private val setter = new TextMapSetter[mutable.Map[String, String]] {
    override def set(carrier: mutable.Map[String, String], key: String, value: String): Unit = {
      carrier += key -> value
    }
  }

  private val getter = new TextMapGetter[mutable.Map[String, String]] {
    override def keys(carrier: mutable.Map[String, String]): java.lang.Iterable[String] = {
      carrier.keySet.asJava
    }

    override def get(carrier: mutable.Map[String, String], key: String): String = {
      carrier.get(key).orNull
    }
  }

  /*
   * The purpose of this is to convert a Span into a Map[String, String]
   */
  def asHeaders(span: Span)(implicit tracer: Tracer): Map[String, String] = {
    val headers: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]
    openTelemetry.getPropagators.getTextMapPropagator.inject(Context.root().`with`(span), headers, setter)
    headers.toMap
  }

  /*
   * Given a traced message, we extract its span context and create a child span.
   * If the traced message doesn't have a span context, we create a whole new span.
   */
  def childFrom(message: TracedMessage[_], operationName: String)(implicit tracer: Tracer): Span = {
    val context: Context = openTelemetry.getPropagators.getTextMapPropagator.extract(Context.root(), message.headers.to(collection.mutable.Map), getter)
    tracer.spanBuilder(operationName).setParent(context).startSpan()
  }

}
