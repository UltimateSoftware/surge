// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import akka.actor.ActorSystem
import com.typesafe.config.{ Config, ConfigFactory }
import surge.core
import surge.core.event.SurgeEventServiceModel
import surge.internal.domain.SurgeEventServiceImpl
import surge.metrics.Metric
import surge.scaladsl.common.HealthCheckTrait

trait SurgeEvent[AggId, Agg, Evt] extends core.SurgeProcessingTrait[Agg, Nothing, Nothing, Evt] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Evt]
  def getMetrics: Vector[Metric]
}

object SurgeEvent {
  def create[AggId, Agg, Evt](businessLogic: SurgeEventBusinessLogic[AggId, Agg, Evt]): SurgeEvent[AggId, Agg, Evt] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    val config = ConfigFactory.load()
    new SurgeEventImpl(actorSystem, SurgeEventServiceModel.apply(businessLogic), businessLogic.aggregateIdToString, config)
  }

}

private[scaladsl] class SurgeEventImpl[AggId, Agg, Evt](
    val actorSystem: ActorSystem,
    override val businessLogic: SurgeEventServiceModel[Agg, Evt],
    aggIdToString: AggId => String,
    config: Config)
    extends SurgeEventServiceImpl[Agg, Evt](actorSystem, businessLogic, config)
    with SurgeEvent[AggId, Agg, Evt] {

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Evt] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  def getMetrics: Vector[Metric] = businessLogic.metrics.getMetrics
}
