// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core
import surge.core.event.SurgeEventServiceModel
import surge.health.config.WindowingStreamConfigLoader
import surge.health.matchers.SignalPatternMatcherRegistry
import surge.internal.domain.SurgeEventServiceImpl
import surge.internal.health.HealthSignalStreamProvider
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.javadsl.common.{ HealthCheck, HealthCheckTrait }
import surge.metrics.Metric

import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

trait SurgeEvent[AggId, Agg, Evt] extends core.SurgeProcessingTrait[Agg, Nothing, Nothing, Evt] with HealthCheckTrait {
  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Evt]
  def getMetrics: java.util.List[Metric]
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
        patternMatchers = SignalPatternMatcherRegistry.load().toSeq),
      businessLogic.aggregateIdToString,
      businessLogic.config)
  }

}

private[javadsl] class SurgeEventImpl[AggId, Agg, Evt](
    val actorSystem: ActorSystem,
    override val businessLogic: SurgeEventServiceModel[Agg, Evt],
    signalStreamProvider: HealthSignalStreamProvider,
    aggIdToString: AggId => String,
    config: Config)
    extends SurgeEventServiceImpl[Agg, Evt](actorSystem, businessLogic, signalStreamProvider, config)
    with SurgeEvent[AggId, Agg, Evt] {

  import surge.javadsl.common.HealthCheck._
  def getHealthCheck: CompletionStage[HealthCheck] = {
    FutureConverters.toJava(healthCheck().map(_.asJava))
  }

  def aggregateFor(aggregateId: AggId): AggregateRef[Agg, Evt] = {
    AggregateRef(aggIdToString(aggregateId), actorRouter.actorRegion, businessLogic.tracer)
  }

  def getMetrics: java.util.List[Metric] = businessLogic.metrics.getMetrics.asJava

  def registerRebalanceListener(listener: ConsumerRebalanceListener[AggId, Agg, Evt]): Unit = {
    registerRebalanceCallback { assignments =>
      val javaAssignments = assignments.partitionAssignments.map(kv => kv._1 -> kv._2.asJava).asJava
      listener.onRebalance(engine = this, javaAssignments)
    }
  }
}
