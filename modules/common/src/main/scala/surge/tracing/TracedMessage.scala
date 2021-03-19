// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.tracing

import java.util

import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.opentracing.propagation.{ Format, TextMapExtract, TextMapInject }
import io.opentracing.{ References, Span, SpanContext, Tracer }
import surge.akka.cluster.JacksonSerializable

import scala.jdk.CollectionConverters._

object TracedMessage {
  def apply[T](tracer: Tracer, message: T, span: Span): TracedMessage[T] = {
    val headers: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]
    tracer.inject(span.context, Format.Builtin.TEXT_MAP_INJECT, new TextMapInject {
      override def put(key: String, value: String): Unit = headers.put(key, value)
    })
    span.context()
    TracedMessage(message, headers.toMap)
  }
}

case class TracedMessage[T](
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "messageType", visible = true) message: T,
    headers: Map[String, String]) extends JacksonSerializable {
  private def spanContext(tracer: Tracer): Option[SpanContext] = {
    Option(tracer.extract(Format.Builtin.TEXT_MAP_EXTRACT, new TextMapExtract {
      override def iterator(): util.Iterator[util.Map.Entry[String, String]] = headers.asJava.entrySet().iterator()
    }))
  }

  def activeSpan(tracer: Tracer): Span = {
    val spanBuilder = tracer.buildSpan("receive")
      .ignoreActiveSpan()
    spanContext(tracer).foreach(ctx => spanBuilder.addReference(References.FOLLOWS_FROM, ctx))
    spanBuilder.start()
  }
}
