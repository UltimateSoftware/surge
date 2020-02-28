// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.actor.ActorRef
import com.ultimatesoftware.kafka.streams.core.{ AggregateRefTrait, GenericAggregateActor }

import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext

trait AggregateRef[AggIdType, Agg, Cmd, Meta] {
  def getState: CompletionStage[Optional[Agg]]
  def ask(commandProps: Meta, command: Cmd): CompletionStage[CommandResult[Agg]]
}

final class AggregateRefImpl[AggIdType, Agg, Cmd, CmdMeta](
    val aggregateId: AggIdType,
    val region: ActorRef) extends AggregateRef[AggIdType, Agg, Cmd, CmdMeta] with AggregateRefTrait[AggIdType, Agg, Cmd, CmdMeta] {

  private implicit val ec: ExecutionContext = ExecutionContext.global

  def getState: CompletionStage[Optional[Agg]] = {
    FutureConverters.toJava(queryState.map(_.asJava))
  }

  def ask(commandProps: CmdMeta, command: Cmd): CompletionStage[CommandResult[Agg]] = {
    val envelope = GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta](aggregateId, commandProps, command)
    val result = askWithRetries(envelope).map {
      case Left(error) ⇒
        CommandFailure[Agg](error)
      case Right(aggOpt) ⇒
        CommandSuccess[Agg](aggOpt.asJava)
    }
    FutureConverters.toJava(result)
  }
}
