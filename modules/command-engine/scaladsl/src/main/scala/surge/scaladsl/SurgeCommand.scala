// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scaladsl

import akka.actor.ActorSystem
import surge.core
import surge.metrics.Metric

trait SurgeCommand[AggId, Agg, Command, Event]
  extends core.SurgeCommandTrait[Agg, Command, Event]
  with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Event]
  def getMetrics: Seq[Metric] = businessLogic.metrics.getMetrics
}

object SurgeCommand {
  def apply[AggId, Agg, Command, Event](
    businessLogic: SurgeCommandBusinessLogic[AggId, Agg, Command, Event]): SurgeCommand[AggId, Agg, Command, Event] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    apply(actorSystem, businessLogic)
  }
  def apply[AggId, Agg, Command, Event](
    actorSystem: ActorSystem,
    businessLogic: SurgeCommandBusinessLogic[AggId, Agg, Command, Event]): SurgeCommand[AggId, Agg, Command, Event] = {
    new SurgeCommandImpl(actorSystem, SurgeCommandBusinessLogic.toCore(businessLogic), businessLogic.aggregateIdToString)
  }
}

private[scaladsl] class SurgeCommandImpl[AggId, Agg, Command, Event](
    val actorSystem: ActorSystem,
    override val businessLogic: core.SurgeCommandBusinessLogic[Agg, Command, Event],
    aggIdToString: AggId => String)
  extends core.SurgeCommandImpl[Agg, Command, Event](actorSystem, businessLogic)
  with SurgeCommand[AggId, Agg, Command, Event] {

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Event] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  override def getMetrics: Seq[Metric] = businessLogic.metrics.getMetrics
}
