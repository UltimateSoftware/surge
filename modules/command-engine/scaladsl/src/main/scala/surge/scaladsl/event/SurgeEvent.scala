// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core
import surge.core.event.SurgeEventServiceModel
import surge.health.config.WindowingStreamConfigLoader
import surge.health.matchers.SignalPatternMatcherRegistry
import surge.internal.domain.SurgeEventServiceImpl
import surge.internal.health.HealthSignalStreamProvider
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.metrics.Metric
import surge.scaladsl.common.HealthCheckTrait

trait SurgeEvent[AggId, Agg, Evt] extends core.SurgeProcessingTrait[Agg, Nothing, Nothing, Evt, Agg] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Evt, Agg]
  def getMetrics: Vector[Metric]
  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Evt]): Unit
}

object SurgeEvent {
  def create[AggId, Agg, Evt](businessLogic: SurgeEventBusinessLogic[AggId, Agg, Evt]): SurgeEvent[AggId, Agg, Evt] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    new SurgeEventImpl(
      actorSystem,
      SurgeEventServiceModel.apply(businessLogic),
      new SlidingHealthSignalStreamProvider(
        WindowingStreamConfigLoader.load(businessLogic.config),
        actorSystem,
        filters = SignalPatternMatcherRegistry.load().toSeq),
      businessLogic.aggregateIdToString,
      businessLogic.config)
  }

}

private[scaladsl] class SurgeEventImpl[AggId, Agg, Evt](
    val actorSystem: ActorSystem,
    override val businessLogic: SurgeEventServiceModel[Agg, Evt, Agg],
    signalStreamProvider: HealthSignalStreamProvider,
    aggIdToString: AggId => String,
    config: Config)
    extends SurgeEventServiceImpl[Agg, Evt, Agg](actorSystem, businessLogic, signalStreamProvider, config)
    with SurgeEvent[AggId, Agg, Evt] {

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Evt, Agg] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  def getMetrics: Vector[Metric] = businessLogic.metrics.getMetrics

  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Evt]): Unit = {
    registerRebalanceCallback { assignments => listener.onRebalance(engine = this, assignments.partitionAssignments) }
  }
}
