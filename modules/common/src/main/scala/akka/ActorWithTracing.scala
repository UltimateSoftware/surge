// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package akka

import akka.actor.ActorRef
import surge.tracing.{ SpanExtensions, TracePropagation, TracedMessage }

trait ActorWithTracing extends AroundReceiveActor with ActorOps with SpanExtensions {

  type MessageNameExtractor = PartialFunction[Any, String]

  protected var activeSpan: Span = _

  implicit val tracer: Tracer

  private def actorClassFullName: String = this.getClass.getName

  private def actorClassSimpleName: String = this.getClass.getSimpleName

  def messageNameForTracedMessages: MessageNameExtractor = PartialFunction.empty[Any, String]

  private def getMessageName(msg: Any): String = {
    if (messageNameForTracedMessages.isDefinedAt(msg)) {
      messageNameForTracedMessages(msg)
    } else {
      msg.getClass.getSimpleName
    }
  }

  private def getFields: Map[String, String] = {
    Map(
      "actor" -> actorClassSimpleName,
      "actor (fqcn)" -> actorClassFullName,
      "receiver path" -> self.prettyPrintPath,
      "sender path" -> sender().prettyPrintPath)
  }

  override def doAroundReceive(receive: Receive, msg: Any): Unit = {
    msg match {
      case tracedMsg: TracedMessage[_] =>
        val messageName: String = getMessageName(tracedMsg.message)
        val operationName: String = s"$actorClassSimpleName:$messageName"
        val newSpan: Span = TracePropagation.childFrom(tracedMsg, operationName)
        activeSpan = newSpan
        val fields: Map[String, String] = getFields +
          ("message" -> messageName) +
          ("message (fqcn)" -> tracedMsg.message.getClass.getName)
        newSpan.log("receive", fields)
        superAroundReceive(receive, tracedMsg.message)
      case other =>
        val messageName: String = other.getClass.getSimpleName
        val operationName: String = s"$actorClassSimpleName:$messageName"
        val newSpan: Span = tracer.buildSpan(operationName).start()
        activeSpan = newSpan
        val fields: Map[String, String] = getFields +
          ("message" -> messageName) + ("messageName (fqcn)" -> other.getClass.getName)
        newSpan.log("receive", fields)
        superAroundReceive(receive, msg)
    }
  }

  override def afterReceive(receive: Receive, msg: Any): Unit = {
    activeSpan.log("done")
    activeSpan.finish()
  }

}

trait ActorOps {
  implicit class ActorRefExtension(val actRef: ActorRef) {
    // pretty print actor path so we can include it in the OpenTracing annotations
    def prettyPrintPath: String = actRef.path.toStringWithAddress(actRef.path.address)
  }
  // TODO: add other methods here such forwardAndTrace, tellAndTrace, askAndTrace
}
