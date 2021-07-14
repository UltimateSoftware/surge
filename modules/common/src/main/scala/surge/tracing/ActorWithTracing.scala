// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import akka.actor.{ Actor, ActorRef }
import io.opentracing.{ Span, Tracer }
import surge.internal.utils.SpanExtensions

// format: off
/*
 How to use this trait:

 class MyActor extends ActorWithTracing {

   // optional
   override def extractMessageName = {
     case m: Envelope[_] => s"Envelope[${m.payload.getClass.getSimpleName}]"
   }

   override def receive: Receive = traceableMessages {
     span => {
       case e: Envelope[_] => // Envelope[DoWork]
        // the new span is named "MyActor:Envelope[DoWork]"
        doSomeWork()
        span.log("some work done!")
        doEvenMoreWork()
        span.log("even more work done!")
        otherActor ! TracedMessage(WorkDone(), parentSpan = span)
        // no need to call span.finish() - we call it for you automatically
     }
  }.orElse {
    // messages we do not want to trace go here
    case OtherMessage => doStuff()
  }
 }
*/
// format: on

trait ActorWithTracing extends Actor with ActorOps with SpanExtensions {

  implicit val tracer: Tracer

  private def actorClassFullName: String = this.getClass.getName

  private def actorClassSimpleName: String = this.getClass.getSimpleName

  def extractMessageName: PartialFunction[Any, String] = PartialFunction.empty[Any, String]

  private def getMessageName(msg: Any): String = {
    if (extractMessageName.isDefinedAt(msg)) {
      extractMessageName(msg)
    } else {
      msg.getClass.getSimpleName
    }
  }

  def traceableMessages(userReceive: ActorReceiveSpan => Actor.Receive): Actor.Receive = new Actor.Receive {
    override def isDefinedAt(m: Any): Boolean = m match {
      case s: TracedMessage[_] => true
      case _                   => false
    }

    override def apply(msg: Any): Unit = {
      msg match {
        case tracedMsg: TracedMessage[_] =>
          val messageName: String = getMessageName(tracedMsg.message)
          val operationName: String = s"$actorClassSimpleName:$messageName"
          val span: Span = Tracing.childFrom(tracedMsg, operationName)
          val actorReceiveSpan = ActorReceiveSpan(span, tracedMsg.messageName)
          val fields = Map(
            "actor" -> actorClassSimpleName,
            "actor (fqcn)" -> actorClassFullName,
            "message" -> messageName,
            "message (fqcn)" -> tracedMsg.message.getClass.getName,
            "receiver path" -> self.prettyPrintPath,
            "sender path" -> sender().prettyPrintPath)
          if (userReceive(actorReceiveSpan).isDefinedAt(tracedMsg.message)) {
            span.log("receive", fields)
            userReceive(actorReceiveSpan)(tracedMsg.message)
            span.log("done")
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

final class ActorReceiveSpan private (private val innerSpan: Span, val messageName: String) {

  private[tracing] def getUnderlyingSpan: Span = innerSpan // solely used by the unit test

  def log(event: String, fields: Map[String, String]): Unit = {
    import SpanExtensions._
    innerSpan.log(event, fields)
  }

  def startChildSpan(operationName: String)(implicit tracer: Tracer): Span = {
    tracer.buildSpan(operationName).asChildOf(innerSpan).start()
  }
}

object ActorReceiveSpan {
  def apply(span: Span, messageName: String): ActorReceiveSpan = new ActorReceiveSpan(span, messageName)
}

trait ActorOps {
  implicit class ActorRefExtension(val actRef: ActorRef) {
    import SpanExtensions._
    // pretty print actor path so we can include it in the OpenTracing annotations
    def prettyPrintPath: String = actRef.path.toStringWithAddress(actRef.path.address)
  }
}
