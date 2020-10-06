// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.{ HealthCheck, HealthyComponent, core }

trait KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta]
  extends core.KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta]
  with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, Agg, Command, CmdMeta, Event, EvtMeta]
}

object KafkaStreamsCommand {
  def apply[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]): KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    apply(actorSystem, businessLogic)
  }
  def apply[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    actorSystem: ActorSystem,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]): KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {
    new KafkaStreamsCommandImpl(actorSystem, businessLogic.toCore)
  }
}

private[scaladsl] class KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    val actorSystem: ActorSystem,
    override val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta])
  extends core.KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta](actorSystem, businessLogic)
  with KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {

  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, Agg, Command, CmdMeta, Event, EvtMeta] = {
    new AggregateRefImpl(aggregateId, actorRouter.actorRegion)
  }
}
