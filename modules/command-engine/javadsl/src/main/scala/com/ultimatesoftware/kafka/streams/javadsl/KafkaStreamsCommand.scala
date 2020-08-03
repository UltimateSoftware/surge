// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.concurrent.CompletionStage
import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.core
import scala.compat.java8.FutureConverters
import com.ultimatesoftware.kafka.streams.javadsl.HealthCheck._
import scala.concurrent.ExecutionContext.Implicits.global

trait KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] extends core.KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, Agg, Command, CmdMeta, Event, EvtMeta]
}

object KafkaStreamsCommand {
  def create[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]): KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    create(actorSystem, businessLogic)
  }

  def create[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    actorSystem: ActorSystem,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]): KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {
    new KafkaStreamsCommandImpl(actorSystem, businessLogic.toCore)
  }
}

private[javadsl] class KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    val actorSystem: ActorSystem,
    override val businessLogic: core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta])
  extends core.KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta](actorSystem, businessLogic)
  with KafkaStreamsCommand[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {

  def getHealthCheck(): CompletionStage[HealthCheck] = {
    FutureConverters.toJava(healthCheck().map(_.asJava))
  }

  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, Agg, Command, CmdMeta, Event, EvtMeta] = {
    new AggregateRefImpl(aggregateId, actorRouter.actorRegion)
  }
}
