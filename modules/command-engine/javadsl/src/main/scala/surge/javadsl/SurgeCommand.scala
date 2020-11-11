// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.javadsl

import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
import surge.core
import surge.javadsl.HealthCheck._

import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext.Implicits.global

trait SurgeCommand[AggId, Agg, Command, Event] extends core.SurgeCommandTrait[Agg, Command, Event] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Event]
}

object SurgeCommand {
  def create[AggId, Agg, Command, Event](
    businessLogic: SurgeCommandBusinessLogic[AggId, Agg, Command, Event]): SurgeCommand[AggId, Agg, Command, Event] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    create(actorSystem, businessLogic)
  }

  def create[AggId, Agg, Command, Event](
    actorSystem: ActorSystem,
    businessLogic: SurgeCommandBusinessLogic[AggId, Agg, Command, Event]): SurgeCommand[AggId, Agg, Command, Event] = {
    new SurgeCommandImpl(actorSystem, SurgeCommandBusinessLogic.toCore(businessLogic), businessLogic.aggregateIdToString)
  }
}

private[javadsl] class SurgeCommandImpl[AggId, Agg, Command, Event](
    val actorSystem: ActorSystem,
    override val businessLogic: core.SurgeCommandBusinessLogic[Agg, Command, Event],
    aggIdToString: AggId ⇒ String)
  extends core.SurgeCommandImpl[Agg, Command, Event](actorSystem, businessLogic)
  with SurgeCommand[AggId, Agg, Command, Event] {

  def getHealthCheck(): CompletionStage[HealthCheck] = {
    FutureConverters.toJava(healthCheck().map(_.asJava))
  }

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Event] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion)
  }
}
