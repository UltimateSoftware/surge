// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.core

trait KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] extends core.KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {
  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, Agg, Command, CmdMeta]
}

object KafkaStreamsCommand {
  def create[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]): KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    new KafkaStreamsCommandImpl(actorSystem, businessLogic.toCore)
  }
}

private[javadsl] class KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    val actorSystem: ActorSystem,
    val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta])
  extends KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] with core.KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {

  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, Agg, Command, CmdMeta] = {
    new AggregateRefImpl(aggregateId, actorRouter.actorRegion, system)
  }
}
