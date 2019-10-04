// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.actor.{ ActorRef, ActorSystem }
import com.ultimatesoftware.kafka.streams.core.{ AggregateRefTrait, GenericAggregateActor }

import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext

final class AggregateRef[AggIdType, Agg, Cmd, CmdMeta](
    val aggregateId: AggIdType,
    val region: ActorRef,
    val system: ActorSystem) extends AggregateRefTrait[AggIdType, Agg, Cmd, CmdMeta] {

  private implicit val ec: ExecutionContext = ExecutionContext.global

  def getState: CompletionStage[Optional[Agg]] = {
    FutureConverters.toJava(queryState.map(_.asJava))
  }

  def ask(commandProps: CmdMeta, command: Cmd): CompletionStage[Optional[Agg]] = {
    val envelope = GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta](aggregateId, commandProps, command)
    val result = askWithRetries(envelope, 0).map { result ⇒
      result.toOption.flatten.asJava
    }
    FutureConverters.toJava(result)
  }
}
