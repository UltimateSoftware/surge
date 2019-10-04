// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.{ ActorRef, ActorSystem }
import com.ultimatesoftware.kafka.streams.core.{ AggregateRefTrait, GenericAggregateActor }
import com.ultimatesoftware.scala.core.validations

import scala.concurrent.{ ExecutionContext, Future }

final class AggregateRef[AggIdType, Agg, Cmd, CmdMeta](
    val aggregateId: AggIdType,
    val region: ActorRef,
    val system: ActorSystem) extends AggregateRefTrait[AggIdType, Agg, Cmd, CmdMeta] {

  def ask(commandProps: CmdMeta, command: Cmd)(implicit ec: ExecutionContext): Future[Either[Seq[validations.ValidationError], Option[Agg]]] = {
    val envelope = GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta](aggregateId, commandProps, command)
    askWithRetries(envelope, 0)
  }

  def getState: Future[Option[Agg]] = queryState
}
