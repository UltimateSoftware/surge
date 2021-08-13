// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core
import surge.core.command.SurgeCommandModel
import surge.core.commondsl.{ SurgeCommandBusinessLogicTrait, SurgeRejectableCommandBusinessLogicTrait }
import surge.health.config.WindowingStreamConfigLoader
import surge.health.matchers.SignalPatternMatcherRegistry
import surge.internal.domain
import surge.internal.health.HealthSignalStreamProvider
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.metrics.Metric
import surge.scaladsl.common.HealthCheckTrait

trait SurgeCommand[AggId, Agg, Command, Rej, Evt, Response] extends core.SurgeProcessingTrait[Agg, Command, Rej, Evt, Response] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Evt, Response]
  def getMetrics: Seq[Metric] = businessLogic.metrics.getMetrics
  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Command, Rej, Evt, Response]): Unit
}

object SurgeCommand {
  def apply[AggId, Agg, Command, Event, Response](
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event, Response]): SurgeCommand[AggId, Agg, Command, Nothing, Event, Response] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    apply(actorSystem, businessLogic, businessLogic.config)
  }
  def apply[AggId, Agg, Command, Event, Response](
      actorSystem: ActorSystem,
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event, Response],
      config: Config): SurgeCommand[AggId, Agg, Command, Nothing, Event, Response] = {
    new SurgeCommandImpl(
      actorSystem,
      SurgeCommandModel(businessLogic),
      new SlidingHealthSignalStreamProvider(WindowingStreamConfigLoader.load(config), actorSystem, filters = SignalPatternMatcherRegistry.load().toSeq),
      businessLogic.aggregateIdToString,
      config)
  }

  def apply[AggId, Agg, Command, Rej, Evt, Response](
      actorSystem: ActorSystem,
      businessLogic: SurgeRejectableCommandBusinessLogicTrait[AggId, Agg, Command, Rej, Evt, Response],
      config: Config): SurgeCommand[AggId, Agg, Command, Rej, Evt, Response] = {
    new SurgeCommandImpl(
      actorSystem,
      SurgeCommandModel(businessLogic),
      new SlidingHealthSignalStreamProvider(WindowingStreamConfigLoader.load(config), actorSystem, filters = SignalPatternMatcherRegistry.load().toSeq),
      businessLogic.aggregateIdToString,
      config)
  }
}

private[scaladsl] class SurgeCommandImpl[AggId, Agg, Command, Rej, Event, Response](
    val actorSystem: ActorSystem,
    override val businessLogic: SurgeCommandModel[Agg, Command, Rej, Event, Response],
    signalStreamProvider: HealthSignalStreamProvider,
    aggIdToString: AggId => String,
    override val config: Config)
    extends domain.SurgeCommandImpl[Agg, Command, Rej, Event, Response](actorSystem, businessLogic, signalStreamProvider, config)
    with SurgeCommand[AggId, Agg, Command, Rej, Event, Response] {

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Event, Response] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  override def getMetrics: Seq[Metric] = businessLogic.metrics.getMetrics

  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Command, Rej, Event, Response]): Unit = {
    registerRebalanceCallback { assignments => listener.onRebalance(engine = this, assignments.partitionAssignments) }
  }
}
