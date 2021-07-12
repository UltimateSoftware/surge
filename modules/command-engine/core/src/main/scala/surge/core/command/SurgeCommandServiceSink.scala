// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.command

import surge.internal.utils.Logging
import surge.streams.{ EventSink, EventSinkSupport }

import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag

trait SurgeMultiCommandServiceSink[AggId, Command, Event] extends EventSink[Event] with EventSinkSupport[Event] with Logging {

  def eventToCommands: Event => Seq[Command]
  implicit def executionContext: ExecutionContext
  protected def sendToAggregate(aggId: AggId, command: Command): Future[Any]

  def aggregateIdFromCommand: Command => AggId

  override def handleEvent(key: String, event: Event, headers: Map[String, Array[Byte]]): Future[Any] = {
    val aggregateCommands = eventToCommands(event).map { cmd =>
      (aggregateIdFromCommand(cmd), cmd)
    }

    if (aggregateCommands.isEmpty) {
      log.info(s"Skipping event with class ${event.getClass} since it was not converted into a command")
    }
    val appliedCommands = aggregateCommands.map { case (aggregateId, command) =>
      sendToAggregate(aggregateId, command)
    }
    Future.sequence(appliedCommands)
  }
}

trait SurgeCommandServiceSink[AggId, Command, Event] extends SurgeMultiCommandServiceSink[AggId, Command, Event] {
  def eventToCommand: Event => Option[Command]
  override def eventToCommands: Event => Seq[Command] = eventToCommand.andThen(_.toSeq)
}
