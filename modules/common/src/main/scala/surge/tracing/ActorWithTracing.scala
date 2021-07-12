// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import akka.actor.Actor
import io.opentracing.{Span, Tracer}

case class ActorReceiveSpan private(span: Span) {

  import SpanOps._

  def log(event: String, fields: Map[String, String]): Unit = {
    span.log(event, fields)
  }

  def startChildSpan(operationName: String)(implicit tracer: Tracer): Span = {
    tracer.buildSpan(operationName).asChildOf(span).start()
  }
}

object ActorReceiveSpan {
  def apply(span: Span): ActorReceiveSpan = new ActorReceiveSpan(span)
}


trait ActorWithTracing extends Actor with ActorOps with SpanOps {

  implicit val tracer: Tracer = ???

  def traceableMessages(userReceive: ActorReceiveSpan => Actor.Receive): Actor.Receive = {
    case tracedMsg: Any if tracedMsg.isInstanceOf[TracedMessage] =>
      val span: Span = TracedMessage.followFrom(tracedMsg, operationName = s"${this.getClass.getName}:${tracedMsg.msg.getClass.getSimpleName}")
      val actorReceiveSpan = ActorReceiveSpan(span)
      val fields = Map(
        "receiver" -> this.getClass.getSimpleName,
        "receiver path" -> self.prettyPrintPath,
        "sender path" -> sender().prettyPrintPath,
        "message" -> tracedMsg.getClass.getName)
      if (userReceive(actorReceiveSpan).isDefinedAt(tracedMsg.msg)) {
        span.log(s"receive", fields)
        userReceive(ActorReceiveSpan(span))(tracedMsg.msg)
        span.log(s"done")
        span.finish()
      } else {
        context.system.deadLetters ! tracedMsg
      }
  }
}

