// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.core
import com.ultimatesoftware.scala.core.domain.StatePlusMetadata
import com.ultimatesoftware.scala.core.messaging.{ EventMessage, EventProperties }

trait UltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta]
  extends KafkaStreamsCommand[AggId, StatePlusMetadata[Agg], Cmd, EventMessage[Event], CmdMeta, EventProperties]

object UltiKafkaStreamsCommand {
  def apply[AggId, Agg, Cmd, Event, CmdMeta](
    ultiBusinessLogic: UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta]): UltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta] = {

    val actorSystem = ActorSystem(s"${ultiBusinessLogic.aggregateName}ActorSystem")
    new UltiKafkaStreamsCommandImpl(actorSystem, ultiBusinessLogic.toCore)
  }
}

private[scaladsl] class UltiKafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta](
    val actorSystem: ActorSystem,
    val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, StatePlusMetadata[Agg], Command, EventMessage[Event], CmdMeta, EventProperties]) extends UltiKafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta]
