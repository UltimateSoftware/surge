// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.ActorRef
import com.ultimatesoftware.kafka.streams.core.{ AggregateRefTrait, GenericAggregateActor }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRef[AggIdType, Agg, Cmd, CmdMeta] {
  def ask(commandProps: CmdMeta, command: Cmd)(implicit ec: ExecutionContext): Future[CommandResult[Agg]]
  def getState: Future[Option[Agg]]
}

final class AggregateRefImpl[AggIdType, Agg, Cmd, CmdMeta](
    val aggregateId: AggIdType,
    val region: ActorRef) extends AggregateRef[AggIdType, Agg, Cmd, CmdMeta] with AggregateRefTrait[AggIdType, Agg, Cmd, CmdMeta] {

  def ask(commandProps: CmdMeta, command: Cmd)(implicit ec: ExecutionContext): Future[CommandResult[Agg]] = {
    val envelope = GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta](aggregateId, commandProps, command)
    askWithRetries(envelope).map {
      case Left(error) ⇒
        CommandFailure(error)
      case Right(aggOpt) ⇒
        CommandSuccess(aggOpt)
    }
  }

  def getState: Future[Option[Agg]] = queryState
}
