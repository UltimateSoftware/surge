// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import akka.actor.ActorSystem
import com.typesafe.config.{ Config, ConfigFactory }
import surge.core
import surge.core.{ SurgeCommandModel => CoreBusinessLogic }
import surge.internal.commondsl.command.{ SurgeCommandBusinessLogic, SurgeRejectableCommandBusinessLogic }
import surge.javadsl.common.{ HealthCheck, HealthCheckTrait }
import surge.metrics.Metric

import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

trait SurgeCommand[AggId, Agg, Command, +Rej, Evt] extends core.SurgeProcessingTrait[Agg, Command, Rej, Evt] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Evt]
  def getMetrics: java.util.List[Metric]
}

object SurgeCommand {
  def create[AggId, Agg, Command, Evt](businessLogic: SurgeCommandBusinessLogic[AggId, Agg, Command, Evt]): SurgeCommand[AggId, Agg, Command, Nothing, Evt] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    val config = ConfigFactory.load()
    create(actorSystem, businessLogic, config)
  }

  def create[AggId, Agg, Command, Evt](
      actorSystem: ActorSystem,
      businessLogic: SurgeCommandBusinessLogic[AggId, Agg, Command, Evt],
      config: Config): SurgeCommand[AggId, Agg, Command, Nothing, Evt] = {
    new SurgeCommandImpl(actorSystem, SurgeCommandBusinessLogic.toCore(businessLogic), businessLogic.aggregateIdToString, config)
  }

  def create[AggId, Agg, Command, Rej, Evt](
      actorSystem: ActorSystem,
      businessLogic: SurgeRejectableCommandBusinessLogic[AggId, Agg, Command, Rej, Evt],
      config: Config): SurgeCommand[AggId, Agg, Command, Rej, Evt] = {
    new SurgeCommandImpl(actorSystem, SurgeRejectableCommandBusinessLogic.toCore(businessLogic), businessLogic.aggregateIdToString, config)
  }
}

private[javadsl] class SurgeCommandImpl[AggId, Agg, Command, +Rej, Evt](
    val actorSystem: ActorSystem,
    override val businessLogic: CoreBusinessLogic[Agg, Command, Rej, Evt],
    aggIdToString: AggId => String,
    config: Config)
    extends core.SurgeCommandImpl[Agg, Command, Rej, Evt](actorSystem, businessLogic, config)
    with SurgeCommand[AggId, Agg, Command, Rej, Evt] {

  import surge.javadsl.common.HealthCheck._
  def getHealthCheck: CompletionStage[HealthCheck] = {
    FutureConverters.toJava(healthCheck.map(_.asJava))
  }

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Evt] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  def getMetrics: java.util.List[Metric] = businessLogic.metrics.getMetrics.asJava
}
