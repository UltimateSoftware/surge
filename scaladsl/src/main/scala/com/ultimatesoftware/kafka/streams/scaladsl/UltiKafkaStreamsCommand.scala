// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

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
    apply(actorSystem, ultiBusinessLogic)
  }

  def apply[AggId, Agg, Cmd, Event, CmdMeta](
    actorSystem: ActorSystem,
    ultiBusinessLogic: UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta]): UltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta] = {
    new UltiKafkaStreamsCommandImpl(actorSystem, ultiBusinessLogic.toCore)

  }
}

private[scaladsl] class UltiKafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta](
    val actorSystem: ActorSystem,
    val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, StatePlusMetadata[Agg], Command, EventMessage[Event], CmdMeta, EventProperties])
  extends UltiKafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta]
  with core.KafkaStreamsCommandImpl[AggId, StatePlusMetadata[Agg], Command, EventMessage[Event], CmdMeta, EventProperties] {

  override def aggregateFor(aggregateId: AggId): AggregateRef[AggId, StatePlusMetadata[Agg], Command, CmdMeta] = {
    new AggregateRefImpl(aggregateId, actorRouter.actorRegion, actorSystem)
  }
}
