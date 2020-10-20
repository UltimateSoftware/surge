// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.ActorRef
import com.ultimatesoftware.kafka.streams.core.{ AggregateRefTrait, GenericAggregateActor }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRef[AggIdType, Agg, Cmd, CmdMeta, Event, EvtMeta] {
  def ask(commandProps: CmdMeta, command: Cmd)(implicit ec: ExecutionContext): Future[CommandResult[Agg]]
  def getState(implicit ec: ExecutionContext): Future[Option[Agg]]
  def applyEvent(event: Event, eventMeta: EvtMeta)(implicit ec: ExecutionContext): Future[ApplyEventResult[Agg]]
}

final class AggregateRefImpl[AggIdType, Agg, Cmd, CmdMeta, Event, EvtMeta](
    val aggregateId: AggIdType,
    val region: ActorRef) extends AggregateRef[AggIdType, Agg, Cmd, CmdMeta, Event, EvtMeta]
  with AggregateRefTrait[AggIdType, Agg, Cmd, CmdMeta, Event, EvtMeta] {

  def ask(commandProps: CmdMeta, command: Cmd)(implicit ec: ExecutionContext): Future[CommandResult[Agg]] = {
    val envelope = GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta](aggregateId, commandProps, command)
    askWithRetries(envelope).map {
      case Left(error) ⇒
        CommandFailure(error)
      case Right(aggOpt) ⇒
        CommandSuccess(aggOpt)
    }
  }

  def getState(implicit ec: ExecutionContext): Future[Option[Agg]] = queryState

  def applyEvent(event: Event, eventMeta: EvtMeta)(implicit ec: ExecutionContext): Future[ApplyEventResult[Agg]] = {
    val envelope = GenericAggregateActor.ApplyEventEnvelope[AggIdType, Event, EvtMeta](aggregateId, event, eventMeta)
    applyEventsWithRetries(envelope)
      .map(aggOpt ⇒ ApplyEventsSuccess[Agg](aggOpt))
      .recover {
        case e ⇒
          ApplyEventsFailure[Agg](e)
      }
  }
}
