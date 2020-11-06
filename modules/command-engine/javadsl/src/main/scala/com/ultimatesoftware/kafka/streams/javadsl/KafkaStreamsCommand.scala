// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.concurrent.CompletionStage
import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.core
import scala.compat.java8.FutureConverters
import com.ultimatesoftware.kafka.streams.javadsl.HealthCheck._
import scala.concurrent.ExecutionContext.Implicits.global

trait KafkaStreamsCommand[AggId, Agg, Command, Event] extends core.KafkaStreamsCommandTrait[Agg, Command, Event] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Event]
}

object KafkaStreamsCommand {
  def create[AggId, Agg, Command, Event](
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event]): KafkaStreamsCommand[AggId, Agg, Command, Event] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    create(actorSystem, businessLogic)
  }

  def create[AggId, Agg, Command, Event](
    actorSystem: ActorSystem,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event]): KafkaStreamsCommand[AggId, Agg, Command, Event] = {
    new KafkaStreamsCommandImpl(actorSystem, businessLogic.toCore, businessLogic.aggregateIdToString)
  }
}

private[javadsl] class KafkaStreamsCommandImpl[AggId, Agg, Command, Event](
    val actorSystem: ActorSystem,
    override val businessLogic: core.KafkaStreamsCommandBusinessLogic[Agg, Command, Event],
    aggIdToString: AggId ⇒ String)
  extends core.KafkaStreamsCommandImpl[Agg, Command, Event](actorSystem, businessLogic)
  with KafkaStreamsCommand[AggId, Agg, Command, Event] {

  def getHealthCheck(): CompletionStage[HealthCheck] = {
    FutureConverters.toJava(healthCheck().map(_.asJava))
  }

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Event] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion)
  }
}
