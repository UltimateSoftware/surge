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

trait SurgeEvent[AggId, Agg, Evt, Response] extends core.SurgeProcessingTrait[Agg, Nothing, Nothing, Evt, Response] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Evt, Response]
  def getMetrics: Vector[Metric]
  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Evt, Response]): Unit
}

object SurgeEvent {
  def create[AggId, Agg, Evt, Response](businessLogic: SurgeEventBusinessLogic[AggId, Agg, Evt, Response]): SurgeEvent[AggId, Agg, Evt, Response] = {
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

private[scaladsl] class SurgeEventImpl[AggId, Agg, Evt, Response](
    val actorSystem: ActorSystem,
    override val businessLogic: SurgeEventServiceModel[Agg, Evt, Response],
    signalStreamProvider: HealthSignalStreamProvider,
    aggIdToString: AggId => String,
    config: Config)
    extends SurgeEventServiceImpl[Agg, Evt, Response](actorSystem, businessLogic, signalStreamProvider, config)
    with SurgeEvent[AggId, Agg, Evt, Response] {

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Evt, Response] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  def getMetrics: Vector[Metric] = businessLogic.metrics.getMetrics

  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Evt, Response]): Unit = {
    registerRebalanceCallback { assignments => listener.onRebalance(engine = this, assignments.partitionAssignments) }
  }
}
