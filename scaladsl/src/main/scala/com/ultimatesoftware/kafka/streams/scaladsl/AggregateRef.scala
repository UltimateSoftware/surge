// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.{ ActorRef, ActorSystem }
import com.ultimatesoftware.kafka.streams.core.{ AggregateRefTrait, GenericAggregateActor }
import com.ultimatesoftware.scala.core.validations

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRef[AggIdType, Agg, Cmd, CmdMeta] {
  def ask(commandProps: CmdMeta, command: Cmd)(implicit ec: ExecutionContext): Future[Option[Agg]]
  def getState: Future[Option[Agg]]
}

final class AggregateRefImpl[AggIdType, Agg, Cmd, CmdMeta](
    val aggregateId: AggIdType,
    val region: ActorRef,
    val system: ActorSystem) extends AggregateRef[AggIdType, Agg, Cmd, CmdMeta] with AggregateRefTrait[AggIdType, Agg, Cmd, CmdMeta] {

  def ask(commandProps: CmdMeta, command: Cmd)(implicit ec: ExecutionContext): Future[Option[Agg]] = {
    val envelope = GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta](aggregateId, commandProps, command)
    askWithRetries(envelope, 0).map { response ⇒
      response.toOption.flatten
    }
  }

  def getState: Future[Option[Agg]] = queryState
}
