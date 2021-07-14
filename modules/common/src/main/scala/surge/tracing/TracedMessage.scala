// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.opentracing.{ Span, Tracer }
import surge.akka.cluster.JacksonSerializable

object TracedMessage {

  def apply[T](message: T, parentSpan: Span)(implicit tracer: Tracer): TracedMessage[T] =
    TracedMessage(message, TracePropagation.asHeaders(parentSpan))

  def apply[T](message: T, parentSpan: ActorReceiveSpan)(implicit tracer: Tracer): TracedMessage[T] =
    TracedMessage(message, parentSpan.getUnderlyingSpan)

}

final case class TracedMessage[T](
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "messageType", visible = true) message: T,
    headers: Map[String, String])
    extends JacksonSerializable
