// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.ActorRef
import com.ultimatesoftware.kafka.streams.core.{ AggregateRefTrait, GenericAggregateActor }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRef[Agg, Cmd, Event] {
  def ask(command: Cmd)(implicit ec: ExecutionContext): Future[CommandResult[Agg]]
  def getState(implicit ec: ExecutionContext): Future[Option[Agg]]
  def applyEvent(event: Event)(implicit ec: ExecutionContext): Future[ApplyEventResult[Agg]]
}

final class AggregateRefImpl[AggId, Agg, Cmd, Event](
    val aggregateId: AggId,
    val region: ActorRef) extends AggregateRef[Agg, Cmd, Event]
  with AggregateRefTrait[AggId, Agg, Cmd, Event] {

  def ask(command: Cmd)(implicit ec: ExecutionContext): Future[CommandResult[Agg]] = {
    val envelope = GenericAggregateActor.CommandEnvelope[Cmd](aggregateId.toString, command)
    askWithRetries(envelope).map {
      case Left(error) ⇒
        CommandFailure(error)
      case Right(aggOpt) ⇒
        CommandSuccess(aggOpt)
    }
  }

  def getState(implicit ec: ExecutionContext): Future[Option[Agg]] = queryState

  def applyEvent(event: Event)(implicit ec: ExecutionContext): Future[ApplyEventResult[Agg]] = {
    val envelope = GenericAggregateActor.ApplyEventEnvelope[Event](aggregateId.toString, event)
    applyEventsWithRetries(envelope)
      .map(aggOpt ⇒ ApplyEventsSuccess[Agg](aggOpt))
      .recover {
        case e ⇒
          ApplyEventsFailure[Agg](e)
      }
  }
}
