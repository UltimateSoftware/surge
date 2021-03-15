// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor._
import akka.pattern.ask
import org.slf4j.LoggerFactory
import surge.internal.config.TimeoutConfig
import surge.kafka.streams._
import surge.kafka.{ KafkaPartitionShardRouterActor, PersistentActorRegionCreator }

import scala.concurrent.{ ExecutionContext, Future }

private[surge] final class SurgePartitionRouter[Agg, Command, Event](
    system: ActorSystem,
    clusterStateTrackingActor: ActorRef,
    businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event],
    regionCreator: PersistentActorRegionCreator[String]) extends HealthyComponent {

  private val log = LoggerFactory.getLogger(getClass)

  val actorRegion: ActorRef = {
    val shardRouterProps = KafkaPartitionShardRouterActor.props(clusterStateTrackingActor, businessLogic.partitioner, businessLogic.kafka.stateTopic,
      regionCreator, RoutableMessage.extractEntityId)
    val actorName = s"${businessLogic.aggregateName}RouterActor"
    system.actorOf(shardRouterProps, name = actorName)
  }

  override def healthCheck(): Future[HealthCheck] = {
    actorRegion
      .ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout * 3)
      .mapTo[HealthCheck]
      .recoverWith {
        case err: Throwable =>
          log.error(s"Failed to get router-actor health check", err)
          Future.successful(
            HealthCheck(
              name = "router-actor",
              id = s"router-actor-${actorRegion.hashCode}",
              status = HealthCheckStatus.DOWN))
      }(ExecutionContext.global)
  }
}
