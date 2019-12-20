// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.core

trait KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope] extends core.KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope] {
  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, Agg, Command, CmdMeta]
}

object KafkaStreamsCommand {
  def apply[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope](
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope]): KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    new KafkaStreamsCommandImpl(actorSystem, businessLogic.toCore)
  }
}

private[scaladsl] class KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope](
    val actorSystem: ActorSystem,
    val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope])
  extends KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope] with core.KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope] {

  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, Agg, Command, CmdMeta] = {
    new AggregateRefImpl(aggregateId, actorRouter.actorRegion)
  }
}
