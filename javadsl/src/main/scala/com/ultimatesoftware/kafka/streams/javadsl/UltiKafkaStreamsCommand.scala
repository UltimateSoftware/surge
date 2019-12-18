// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.core
import com.ultimatesoftware.scala.core.domain.StatePlusMetadata
import com.ultimatesoftware.scala.core.messaging.{ EventMessage, EventProperties }

trait UltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta, Envelope <: com.ultimatesoftware.mp.serialization.envelope.Envelope]
  extends KafkaStreamsCommand[AggId, StatePlusMetadata[Agg], Cmd, EventMessage[Event], CmdMeta, EventProperties, Envelope]

object UltiKafkaStreamsCommand {
  def create[AggId, Agg, Cmd, Event, CmdMeta, Envelope <: com.ultimatesoftware.mp.serialization.envelope.Envelope](
    ultiBusinessLogic: UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta, Envelope]): UltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta, Envelope] = {

    val actorSystem = ActorSystem(s"${ultiBusinessLogic.aggregateName}ActorSystem")
    new UltiKafkaStreamsCommandImpl(actorSystem, ultiBusinessLogic.toCore)
  }
}

private[javadsl] class UltiKafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, Envelope <: com.ultimatesoftware.mp.serialization.envelope.Envelope](
    val actorSystem: ActorSystem,
    val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, StatePlusMetadata[Agg], Command, EventMessage[Event], CmdMeta, EventProperties, Envelope])
  extends UltiKafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, Envelope]
  with core.KafkaStreamsCommandImpl[AggId, StatePlusMetadata[Agg], Command, EventMessage[Event], CmdMeta, EventProperties, Envelope] {

  override def aggregateFor(aggregateId: AggId): AggregateRef[AggId, StatePlusMetadata[Agg], Command, CmdMeta] = {
    new AggregateRefImpl(aggregateId, actorRouter.actorRegion)
  }
}
