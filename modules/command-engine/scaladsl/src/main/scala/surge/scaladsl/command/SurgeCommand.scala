// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import akka.actor.ActorSystem
import com.typesafe.config.{ Config, ConfigFactory }
import surge.core
import surge.core.command
import surge.core.command.SurgeCommandModel
import surge.core.commondsl.{ SurgeCommandBusinessLogicTrait, SurgeRejectableCommandBusinessLogicTrait }
import surge.internal.domain
import surge.metrics.Metric
import surge.scaladsl.common.HealthCheckTrait

trait SurgeCommand[AggId, Agg, Command, +Rej, Evt] extends core.SurgeProcessingTrait[Agg, Command, Rej, Evt] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Evt]
  def getMetrics: Seq[Metric] = businessLogic.metrics.getMetrics
}

object SurgeCommand {
  def apply[AggId, Agg, Command, Event](
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event]): SurgeCommand[AggId, Agg, Command, Nothing, Event] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    val config = ConfigFactory.load()
    apply(actorSystem, businessLogic, config)
  }
  def apply[AggId, Agg, Command, Event](
      actorSystem: ActorSystem,
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event],
      config: Config = ConfigFactory.load()): SurgeCommand[AggId, Agg, Command, Nothing, Event] = {
    new SurgeCommandImpl(actorSystem, SurgeCommandModel(businessLogic), businessLogic.aggregateIdToString, config)
  }
  def apply[AggId, Agg, Command, Rej, Evt](
      actorSystem: ActorSystem,
      businessLogic: SurgeRejectableCommandBusinessLogicTrait[AggId, Agg, Command, Rej, Evt],
      config: Config): SurgeCommand[AggId, Agg, Command, Rej, Evt] = {
    new SurgeCommandImpl(actorSystem, SurgeCommandModel(businessLogic), businessLogic.aggregateIdToString, config)
  }
}

private[scaladsl] class SurgeCommandImpl[AggId, Agg, Command, +Rej, Event](
    val actorSystem: ActorSystem,
    override val businessLogic: SurgeCommandModel[Agg, Command, Rej, Event],
    aggIdToString: AggId => String,
    override val config: Config)
    extends domain.SurgeCommandImpl[Agg, Command, Rej, Event](actorSystem, businessLogic, config)
    with SurgeCommand[AggId, Agg, Command, Rej, Event] {

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Event] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  override def getMetrics: Seq[Metric] = businessLogic.metrics.getMetrics

}
