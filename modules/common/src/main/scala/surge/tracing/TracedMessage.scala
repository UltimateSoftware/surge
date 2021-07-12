// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import java.util
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.opentracing.propagation.{Format, TextMap, TextMapExtract, TextMapInject}
import io.opentracing.{References, Span, SpanContext, Tracer}
import surge.akka.cluster.JacksonSerializable

import scala.jdk.CollectionConverters._

object TracedMessage {
  def apply[T](message: T, span: Span)(implicit tracer: Tracer): TracedMessage[T] = TracedMessage(message, Tracing.asHeaders(span))
}

final case class TracedMessage[T](
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "messageType", visible = true) message: T,
    headers: Map[String, String])
    extends JacksonSerializable

object Tracing {

  /*
   * The purpose of this is to convert a Span into a Map[String, String]
   */
  def asHeaders(span: Span)(implicit tracer: Tracer): Map[String, String] = {
    val headers: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]
    tracer.inject(
      span.context,
      Format.Builtin.TEXT_MAP,
      new TextMap() {
        override def put(key: String, value: String): Unit = headers.put(key, value)
        override def iterator() = throw new UnsupportedOperationException()
      })
    headers.toMap
  }

  /*
   * Given a traced message, we extract its span context and create a child span.
   * If the traced message doesn't have a span context, we create a whole new span.
   */
  def childFrom(message: TracedMessage[_], operationName: String)(implicit tracer: Tracer): Span = {
    Option(
      tracer.extract(
        Format.Builtin.TEXT_MAP,
        new TextMap() {
          override def put(key: String, value: String): Unit = throw new UnsupportedOperationException()

          import java.util.Map
          import java.util
          override def iterator(): util.Iterator[Map.Entry[String, String]] = {
            import scala.jdk.CollectionConverters._
            message.headers.asJava.entrySet().iterator()
          }
        }))
  } match {
    case Some(spanContext: SpanContext) =>
      tracer.buildSpan(operationName).addReference(References.CHILD_OF, spanContext).start()
    case None =>
      tracer.buildSpan(operationName).start()
  }
}
