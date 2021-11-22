// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core
import surge.core.command._
import surge.core.commondsl.SurgeCommandBusinessLogicTrait
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

trait SurgeCommand[AggId, Agg, Command, Evt] extends core.SurgeProcessingTrait[Agg, Command, Evt] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Evt]
  def getMetrics: java.util.List[Metric]
  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Command, Evt]): Unit
}

object SurgeCommand {
  def create[AggId, Agg, Command, Evt](businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt]): SurgeCommand[AggId, Agg, Command, Evt] = {
    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    create(actorSystem, businessLogic, businessLogic.config)
  }

  def create[AggId, Agg, Command, Evt](
      actorSystem: ActorSystem,
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt],
      config: Config): SurgeCommand[AggId, Agg, Command, Evt] = {
    new SurgeCommandImpl(
      actorSystem,
      SurgeCommandModel(businessLogic),
      new SlidingHealthSignalStreamProvider(WindowingStreamConfigLoader.load(config), actorSystem, patternMatchers = SignalPatternMatcherRegistry.load().toSeq),
      businessLogic.aggregateIdToString,
      config)
  }
}

private[javadsl] class SurgeCommandImpl[AggId, Agg, Command, Evt](
    val actorSystem: ActorSystem,
    override val businessLogic: SurgeCommandModel[Agg, Command, Evt],
    signalStreamProvider: HealthSignalStreamProvider,
    aggIdToString: AggId => String,
    config: Config)
    extends domain.SurgeCommandImpl[Agg, Command, Evt](actorSystem, businessLogic, signalStreamProvider, config)
    with SurgeCommand[AggId, Agg, Command, Evt] {

  import surge.javadsl.common.HealthCheck._
  def getHealthCheck: CompletionStage[HealthCheck] = {
    FutureConverters.toJava(healthCheck().map(_.asJava))
  }

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Command, Evt] = {
    new AggregateRefImpl(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  def getMetrics: java.util.List[Metric] = businessLogic.metrics.getMetrics.asJava

  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Command, Evt]): Unit = {
    registerRebalanceCallback { assignments =>
      val javaAssignments = assignments.partitionAssignments.map(kv => kv._1 -> kv._2.asJava).asJava
      listener.onRebalance(engine = this, javaAssignments)
    }
  }
}
