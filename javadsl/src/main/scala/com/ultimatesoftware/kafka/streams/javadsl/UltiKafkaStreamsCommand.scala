// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.javadsl

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.core
import com.ultimatesoftware.scala.core.domain.StatePlusMetadata
import com.ultimatesoftware.scala.core.messaging.{ EventMessage, EventProperties }

trait UltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta]
  extends KafkaStreamsCommand[AggId, StatePlusMetadata[Agg], Cmd, EventMessage[Event], CmdMeta, EventProperties]

object UltiKafkaStreamsCommand {
  def create[AggId, Agg, Cmd, Event, CmdMeta](
    ultiBusinessLogic: UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta]): UltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta] = {
    new UltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta] {
      private val underlying = KafkaStreamsCommand.create(ultiBusinessLogic)

      override val actorSystem: ActorSystem = underlying.actorSystem
      override val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, StatePlusMetadata[Agg], Cmd, EventMessage[Event], CmdMeta, EventProperties] =
        underlying.businessLogic
    }
  }
}

