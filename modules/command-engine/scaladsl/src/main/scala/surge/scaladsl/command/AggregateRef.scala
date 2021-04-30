// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import akka.actor.ActorRef
import io.opentracing.Tracer
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRef[Agg, Cmd, Event] {
  def sendCommand(command: Cmd)(implicit ec: ExecutionContext): Future[CommandResult[Agg]]
  def getState(implicit ec: ExecutionContext): Future[Option[Agg]]
  def applyEvent(event: Event)(implicit ec: ExecutionContext): Future[ApplyEventResult[Agg]]
}

final class AggregateRefImpl[AggId, Agg, Cmd, Event](
    val aggregateId: AggId,
    val region: ActorRef,
    val tracer: Tracer) extends AggregateRef[Agg, Cmd, Event]
  with AggregateRefTrait[AggId, Agg, Cmd, Event] {

  def sendCommand(command: Cmd)(implicit ec: ExecutionContext): Future[CommandResult[Agg]] = {
    val envelope = PersistentActor.ProcessMessage[Cmd](aggregateId.toString, command)
    sendCommandWithRetries(envelope).map {
      case Left(error) =>
        CommandFailure(error)
      case Right(aggOpt) =>
        CommandSuccess(aggOpt)
    }
  }

  def getState(implicit ec: ExecutionContext): Future[Option[Agg]] = queryState

  def applyEvent(event: Event)(implicit ec: ExecutionContext): Future[ApplyEventResult[Agg]] = {
    val envelope = PersistentActor.ApplyEvent[Event](aggregateId.toString, event)
    applyEventsWithRetries(envelope)
      .map(aggOpt => ApplyEventsSuccess[Agg](aggOpt))
      .recover {
        case e =>
          ApplyEventsFailure[Agg](e)
      }
  }
}
