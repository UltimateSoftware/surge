// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }
import surge.scaladsl.common.{ AggregateRefBaseTrait, _ }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRef[Agg, Cmd, Event] {
  def getState: Future[Option[Agg]]
  def sendCommand(command: Cmd): Future[CommandResult[Agg]]
  def applyEvent(event: Event): Future[ApplyEventResult[Agg]]
}

final class AggregateRefImpl[AggId, Agg, Cmd, Event](val aggregateId: AggId, protected val region: ActorRef, protected val tracer: Tracer)
    extends AggregateRef[Agg, Cmd, Event]
    with AggregateRefBaseTrait[AggId, Agg, Cmd, Event]
    with AggregateRefTrait[AggId, Agg, Cmd, Event] {

  private implicit val ec: ExecutionContext = ExecutionContext.global

  def sendCommand(command: Cmd): Future[CommandResult[Agg]] = {
    val envelope = PersistentActor.ProcessMessage[Cmd](aggregateId.toString, command)
    sendCommandWithRetries(envelope).map {
      case Left(error) =>
        CommandFailure[Agg](error)
      case Right(aggOpt) =>
        CommandSuccess[Agg](aggOpt)
    }
  }
}
