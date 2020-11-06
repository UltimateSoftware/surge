// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core
import com.ultimatesoftware.support.Logging
import scala.concurrent.{ ExecutionContext, Future }

trait SurgeMultiCommandServiceSink[Command, Event] extends EventSink[Event] with Logging {

  def eventToCommands: Event ⇒ Seq[Command]
  implicit def executionContext: ExecutionContext
  protected def sendToAggregate(aggId: String, command: Command): Future[Any]

  def aggregateIdFromCommand: Command ⇒ String

  def handleEvent(event: Event): Future[Any] = {
    val aggregateCommands = eventToCommands(event).map { cmd ⇒
      (aggregateIdFromCommand(cmd), cmd)
    }

    if (aggregateCommands.isEmpty) {
      log.info(s"Skipping event with class ${event.getClass} since it was not converted into a command")
    }
    val appliedCommands = aggregateCommands.map {
      case (aggregateId, command) ⇒
        sendToAggregate(aggregateId, command)
    }
    Future.sequence(appliedCommands)
  }
}

trait SurgeCommandServiceSink[Command, Event]
  extends SurgeMultiCommandServiceSink[Command, Event] {
  def eventToCommand: Event ⇒ Option[Command]
  override def eventToCommands: Event ⇒ Seq[Command] = eventToCommand.andThen(_.toSeq)
}
