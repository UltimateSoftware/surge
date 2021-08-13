// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core
import surge.core.command._
import surge.core.commondsl.{ SurgeCommandBusinessLogicTrait, SurgeRejectableCommandBusinessLogicTrait }
import surge.health.config.WindowingStreamConfigLoader
import surge.health.matchers.SignalPatternMatcherRegistry
import surge.internal.domain
import surge.internal.health.HealthSignalStreamProvider
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.javadsl.common.{ HealthCheck, HealthCheckTrait }
import surge.metrics.Metric

import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

trait SurgeCommand[AggId, Agg, Command, Rej, Evt, Response] extends core.SurgeProcessingTrait[Agg, Command, Rej, Evt, Response] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Evt, Response]
  def getMetrics: java.util.List[Metric]
  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Command, Rej, Evt, Response]): Unit
}

object SurgeCommand {
  def create[AggId, Agg, Command, Evt, Response](
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt, Response]): SurgeCommand[AggId, Agg, Command, Nothing, Evt, Response] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    create(actorSystem, businessLogic, businessLogic.config)
  }

  def create[AggId, Agg, Command, Evt, Response](
      actorSystem: ActorSystem,
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt, Response],
      config: Config): SurgeCommand[AggId, Agg, Command, Nothing, Evt, Response] = {
    new SurgeCommandImpl(
      actorSystem,
      SurgeCommandModel(businessLogic),
      new SlidingHealthSignalStreamProvider(WindowingStreamConfigLoader.load(config), actorSystem, filters = SignalPatternMatcherRegistry.load().toSeq),
      businessLogic.aggregateIdToString,
      config)
  }

  def create[AggId, Agg, Command, Rej, Evt, Response](
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

private[javadsl] class SurgeCommandImpl[AggId, Agg, Command, Rej, Evt, Response](
    val actorSystem: ActorSystem,
    override val businessLogic: SurgeCommandModel[Agg, Command, Rej, Evt, Response],
    signalStreamProvider: HealthSignalStreamProvider,
    aggIdToString: AggId => String,
    config: Config)
    extends domain.SurgeCommandImpl[Agg, Command, Rej, Evt, Response](actorSystem, businessLogic, signalStreamProvider, config)
    with SurgeCommand[AggId, Agg, Command, Rej, Evt, Response] {

  import surge.javadsl.common.HealthCheck._
  def getHealthCheck: CompletionStage[HealthCheck] = {
    FutureConverters.toJava(healthCheck().map(_.asJava))
  }

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Evt, Response] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  def getMetrics: java.util.List[Metric] = businessLogic.metrics.getMetrics.asJava

  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Command, Rej, Evt, Response]): Unit = {
    registerRebalanceCallback { assignments =>
      val javaAssignments = assignments.partitionAssignments.map(kv => kv._1 -> kv._2.asJava).asJava
      listener.onRebalance(engine = this, javaAssignments)
    }
  }
}
