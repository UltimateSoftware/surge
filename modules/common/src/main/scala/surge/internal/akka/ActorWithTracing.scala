// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka

import akka.AroundReceiveActor
import akka.actor.ActorRef
import io.opentelemetry.api.trace.{ Span, Tracer }
import surge.internal.tracing.{ TracePropagation, TracedMessage, TracingHelper }

trait ActorWithTracing extends AroundReceiveActor with ActorOps with TracingHelper {

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

  // fields to include in the log span
  private def getFields(messageName: String, message: Any): Map[String, String] = {
    Map(
      "actor" -> actorClassSimpleName,
      "actor (fqcn)" -> actorClassFullName,
      "receiver path" -> self.prettyPrintPath,
      "sender path" -> sender().prettyPrintPath,
      "message" -> messageName,
      "message (fqcn)" -> message.getClass.getName)
  }

  // extract the name of the message and build an operation name
  private def getNames(message: Any): (String, String) = {
    val messageName = getMessageName(message)
    val operationName = s"$actorClassSimpleName:$messageName"
    (messageName, operationName)
  }

  override def doAroundReceive(receive: Receive, msg: Any): Unit = {
    msg match {
      case tracedMsg: TracedMessage[_] =>
        val (messageName: String, operationName: String) = getNames(tracedMsg.message)
        activeSpan = TracePropagation.childFrom(tracedMsg, operationName)
        activeSpan.log("receive", getFields(messageName, tracedMsg.message))
        superAroundReceive(receive, tracedMsg.message)
      case msg =>
        val (messageName: String, operationName: String) = getNames(msg)
        activeSpan = tracer.buildSpan(operationName).start()
        activeSpan.log("receive", getFields(messageName, msg))
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
