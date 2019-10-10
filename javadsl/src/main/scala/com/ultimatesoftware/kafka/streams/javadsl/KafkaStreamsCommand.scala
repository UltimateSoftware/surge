// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.javadsl

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.core

object KafkaStreamsCommand {
  def apply[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]): KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {

    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    new KafkaStreamsCommand(actorSystem, businessLogic.toCore)
  }
}

class KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] private (
    val actorSystem: ActorSystem,
    val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]) extends core.KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {

  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, Agg, Command, CmdMeta] = {
    new AggregateRef(aggregateId, actorRouter.actorRegion, system)
  }
}
