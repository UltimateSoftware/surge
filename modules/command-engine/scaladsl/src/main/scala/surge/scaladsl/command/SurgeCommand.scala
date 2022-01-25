// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core
import surge.core.Ack
import surge.core.command.SurgeCommandModel
import surge.core.commondsl.SurgeCommandBusinessLogicTrait
import surge.health.config.WindowingStreamConfigLoader
import surge.health.matchers.SignalPatternMatcherRegistry
import surge.internal.domain
import surge.internal.health.HealthSignalStreamProvider
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.metrics.Metric
import surge.scaladsl.common.HealthCheckTrait

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import surge.internal.utils.DiagnosticContextFuturePropagation

trait SurgeCommand[AggId, Agg, Command, Evt] extends core.SurgeProcessingTrait[Agg, Command, Evt] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Evt]
  def getMetrics: Seq[Metric] = businessLogic.metrics.getMetrics
  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Command, Evt]): Unit

  def start(): Future[Ack] = controllable.start()
  def stop(): Future[Ack] = controllable.stop()
  def restart(): Future[Ack] = controllable.restart()
  def shutdown(): Future[Ack] = controllable.shutdown()
}

object SurgeCommand {
  def apply[AggId, Agg, Command, Event](businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event])(
      implicit ec: ExecutionContext): SurgeCommand[AggId, Agg, Command, Event] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    apply(actorSystem, businessLogic, businessLogic.config)
  }
  def apply[AggId, Agg, Command, Event](businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event]): SurgeCommand[AggId, Agg, Command, Event] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    apply(actorSystem, businessLogic, businessLogic.config)(DiagnosticContextFuturePropagation.global)
  }
  def apply[AggId, Agg, Command, Event](actorSystem: ActorSystem, businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event], config: Config)(
      implicit ec: ExecutionContext = DiagnosticContextFuturePropagation.global): SurgeCommand[AggId, Agg, Command, Event] = {
    new SurgeCommandImpl(
      actorSystem,
      SurgeCommandModel(businessLogic),
      new SlidingHealthSignalStreamProvider(WindowingStreamConfigLoader.load(config), actorSystem, patternMatchers = SignalPatternMatcherRegistry.load().toSeq),
      businessLogic.aggregateIdToString,
      config)
  }

}

private[scaladsl] class SurgeCommandImpl[AggId, Agg, Command, Event](
    val actorSystem: ActorSystem,
    override val businessLogic: SurgeCommandModel[Agg, Command, Event],
    signalStreamProvider: HealthSignalStreamProvider,
    aggIdToString: AggId => String,
    override val config: Config)(implicit ec: ExecutionContext)
    extends domain.SurgeCommandImpl[Agg, Command, Event](actorSystem, businessLogic, signalStreamProvider, config)
    with SurgeCommand[AggId, Agg, Command, Event] {

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Event] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer, getEngineStatus)
  }

  override def getMetrics: Seq[Metric] = businessLogic.metrics.getMetrics

  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Command, Event]): Unit = {
    registerRebalanceCallback { assignments => listener.onRebalance(engine = this, assignments.partitionAssignments) }
  }
}
