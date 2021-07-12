// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka

import akka.actor.{Actor, ActorRef}
import io.opentracing.{Span, Tracer}
import surge.internal.utils.SpanExtensions
import surge.tracing.{TracedMessage, Tracing}

trait ActorWithTracing extends Actor with ActorOps with SpanExtensions {

  implicit val tracer: Tracer

  private def actorName: String = this.getClass.getName

  private def getMessageName(tracedMsg: TracedMessage[_]): String = {
    // we could potentially use reflection:
    // https://stackoverflow.com/a/60722477
    /*
    import scala.reflect.runtime.universe._
    def getClassName[T](x: T)(implicit tag: TypeTag[T]): String = {
      tag.tpe match { case TypeRef(_, t, args) => s"""${t.name} [${args.mkString(",")}]""" }
    }
    getClassName(tracedMsg)
    */
    tracedMsg.messageName
  }

  def traceableMessages(userReceive: ActorSpan => Actor.Receive): Actor.Receive = new Actor.Receive {
    override def isDefinedAt(m: Any): Boolean = m match {
      case s: TracedMessage[_] => true
      case _                   => false
    }

    override def apply(msg: Any): Unit = {
      msg match {
        case tracedMsg: TracedMessage[_] =>
          val span: Span = Tracing.childFrom(tracedMsg, operationName = s"${actorName}:${getMessageName(tracedMsg)}")
          val actorReceiveSpan = ActorSpan(span)
          val fields = Map(
            "receiver path" -> self.prettyPrintPath,
            "sender path" -> sender().prettyPrintPath,
            "message" -> getMessageName(tracedMsg))
          if (userReceive(actorReceiveSpan).isDefinedAt(tracedMsg.message)) {
            span.log(s"receive", fields)
            userReceive(actorReceiveSpan)(tracedMsg.message)
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

final case class ActorSpan private(innerSpan: Span) {

  private[akka] def getSpan: Span = innerSpan // solely used by the unit test

  def log(event: String, fields: Map[String, String]): Unit = {
    import SpanExtensions._
    innerSpan.log(event, fields)
  }

  def startChildSpan(operationName: String)(implicit tracer: Tracer): Span = {
    tracer.buildSpan(operationName).asChildOf(innerSpan).start()
  }
}

object ActorSpan {
  def apply(span: Span): ActorSpan = new ActorSpan(span)
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
