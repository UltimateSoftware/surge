// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.core

trait KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] extends core.KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {
  val actorSystem: ActorSystem
  val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]
  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, Agg, Command, CmdMeta] = {
    new AggregateRef(aggregateId, actorRouter.actorRegion, system)
  }
}

object KafkaStreamsCommand {
  def create[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]): KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    new KafkaStreamsCommandImpl(actorSystem, businessLogic.toCore)
  }
}

private[scaladsl] class KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    val actorSystem: ActorSystem,
    val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]) extends KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta]
