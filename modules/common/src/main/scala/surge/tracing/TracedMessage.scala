// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.opentracing.{Span, Tracer}
import surge.akka.cluster.JacksonSerializable

object TracedMessage {

  /**
   * @param message
   *   the message
   * @param parentSpan
   *   the *parent* span
   */
  def apply[T](message: T, messageName: String, parentSpan: Span)(implicit tracer: Tracer): TracedMessage[T] =
    TracedMessage(message, messageName, TracePropagation.asHeaders(parentSpan))

  def apply[T](message: T, messageName: String, parentSpan: ActorReceiveSpan)(implicit tracer: Tracer): TracedMessage[T] =
    TracedMessage(message, messageName, parentSpan.getUnderlyingSpan)

  def apply[T](message: T, span: Span)(implicit tracer: Tracer): TracedMessage[T] =
    TracedMessage(message, message.getClass.getSimpleName, span)

  def apply[T](message: T, headers: Map[String, String]): TracedMessage[T] =
    TracedMessage(message, message.getClass.getSimpleName, headers)

}

final case class TracedMessage[T](
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "messageType", visible = true) message: T,
    messageName: String,
    headers: Map[String, String])
    extends JacksonSerializable
