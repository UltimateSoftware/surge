// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import java.util
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.opentracing.propagation.{Format, TextMap, TextMapExtract, TextMapInject}
import io.opentracing.{References, Span, SpanContext, Tracer}
import surge.akka.cluster.JacksonSerializable

import scala.jdk.CollectionConverters._

case class TracedMessage[T](msg: T, headers: Map[String, String])

object TracedMessage {
  def apply[T](message: T, span: Span)(implicit tracer: Tracer): TracedMessage[T] = {
    val headers: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]
    tracer.inject(
      span.context,
      Format.Builtin.TEXT_MAP,
      new TextMap() {
        override def put(key: String, value: String): Unit = headers.put(key, value)

        override def iterator() = throw new UnsupportedOperationException()
      })
    TracedMessage(message, headers.toMap)
  }

  def followFrom(m: TracedMessage[_], operationName: String)(implicit tracer: Tracer): Span = {
    Option(tracer.extract(Format.Builtin.TEXT_MAP, new TextMap() {
      override def put(key: String, value: String): Unit = throw new UnsupportedOperationException()

      override def iterator() = {
        import scala.jdk.CollectionConverters._
        m.headers.asJava.entrySet().iterator()
      }
    }))
  } match {
    case Some(spanContext: SpanContext) =>
      tracer.buildSpan(operationName).addReference(References.CHILD_OF, spanContext).start()
    case None =>
      tracer.buildSpan(operationName).start()

  }
}
