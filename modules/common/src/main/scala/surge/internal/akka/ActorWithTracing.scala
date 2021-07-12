// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka

import akka.AroundReceiveActor
import akka.actor.{Actor, ActorRef}
import io.opentracing.{Scope, Span, Tracer}
import surge.internal.utils.{SpanExtensions, SpanSupport}
import surge.tracing.{TracedMessage, Tracing}

trait ActorWithTracing extends Actor with ActorOps with SpanExtensions {

  implicit val tracer: Tracer

  private def getMessageName(msg: Any): String = {
    msg.getClass.getSimpleName
  }

  def traceableMessages(userReceive: Spanned => Actor.Receive): Actor.Receive = new Actor.Receive {
    override def isDefinedAt(m: Any): Boolean = m match {
      case s: TracedMessage[_] => true
      case _                   => false
    }

    override def apply(msg: Any): Unit = {
      msg match {
        case tracedMsg: TracedMessage[_] =>
          val span: Span = Tracing.childFrom(tracedMsg, operationName = s"${this.getClass.getName}:${getMessageName(msg)}")
          val actorReceiveSpan = Spanned(span)
          val fields = Map(
            "receiver" -> this.getClass.getSimpleName,
            "receiver path" -> self.prettyPrintPath,
            "sender path" -> sender().prettyPrintPath,
            "message" -> getMessageName(msg))
          if (userReceive(actorReceiveSpan).isDefinedAt(msg)) {
            span.log(s"receive", fields)
            userReceive(actorReceiveSpan)(msg)
            span.log(s"done")
            span.finish()
          } else {
            span.log("lost", fields)
            span.finish()
            context.system.deadLetters ! msg
          }
      }
    }
  }
}

final case class Spanned private (span: Span) {

  def log(event: String, fields: Map[String, String]): Unit = {
    import SpanExtensions._
    span.log(event, fields)
  }

  def startChildSpan(operationName: String)(implicit tracer: Tracer): Span = {
    tracer.buildSpan(operationName).asChildOf(span).start()
  }
}

object Spanned {
  def apply(span: Span): Spanned = new Spanned(span)
}

trait ActorOps {
  implicit class ActorRefExtension(val actRef: ActorRef) {

    import SpanExtensions._

    // pretty print actor path so we can include it in the OpenTracing annotations
    def prettyPrintPath: String = actRef.path.toStringWithAddress(actRef.path.address)

    def tellAndTrace(msg: TracedMessage[_], spanToUse: Span)(implicit tracer: Tracer): Unit = {
      spanToUse.log(s"send", Map("destination path" -> actRef.prettyPrintPath, "message" -> msg.getClass.getName))
      actRef ! msg
      spanToUse.finish()
    }
  }
}
