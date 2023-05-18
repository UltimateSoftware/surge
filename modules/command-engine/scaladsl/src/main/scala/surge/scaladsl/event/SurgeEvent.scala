// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core
import surge.core.event.SurgeEventServiceModel
import surge.health.config.WindowingStreamConfigLoader
import surge.health.matchers.SignalPatternMatcherRegistry
import surge.internal.core.ActorSystemBindingHelper
import surge.internal.domain.SurgeEventServiceImpl
import surge.internal.health.HealthSignalStreamProvider
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.metrics.Metric
import surge.scaladsl.common.HealthCheckTrait
import scala.concurrent.ExecutionContext

trait SurgeEvent[AggId, Agg, Evt] extends core.SurgeProcessingTrait[Agg, Nothing, Evt] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Evt]
  def getMetrics: Vector[Metric]
  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Evt]): Unit
}

object SurgeEvent extends ActorSystemBindingHelper {
  def create[AggId, Agg, Evt](businessLogic: SurgeEventBusinessLogic[AggId, Agg, Evt])(implicit ec: ExecutionContext): SurgeEvent[AggId, Agg, Evt] = {
    val actorSystem = sharedActorSystem()
    new SurgeEventImpl(
      actorSystem,
      SurgeEventServiceModel.apply(businessLogic),
      new SlidingHealthSignalStreamProvider(
        WindowingStreamConfigLoader.load(businessLogic.config),
        actorSystem,
        patternMatchers = SignalPatternMatcherRegistry.load().toSeq),
      businessLogic.aggregateIdToString,
      businessLogic.config)
  }

}

private[scaladsl] class SurgeEventImpl[AggId, Agg, Evt](
    val actorSystem: ActorSystem,
    override val businessLogic: SurgeEventServiceModel[Agg, Evt],
    signalStreamProvider: HealthSignalStreamProvider,
    aggIdToString: AggId => String,
    config: Config)(implicit ec: ExecutionContext)
    extends SurgeEventServiceImpl[Agg, Evt](actorSystem, businessLogic, signalStreamProvider, config)
    with SurgeEvent[AggId, Agg, Evt] {

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Evt] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  def getMetrics: Vector[Metric] = businessLogic.metrics.getMetrics

  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Evt]): Unit = {
    registerRebalanceCallback { assignments => listener.onRebalance(engine = this, assignments.partitionAssignments) }
  }
}
