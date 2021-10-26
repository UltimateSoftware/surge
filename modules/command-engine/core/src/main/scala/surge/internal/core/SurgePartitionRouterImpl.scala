// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.core

import java.util.regex.Pattern
import akka.actor._
import akka.pattern.ask
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import surge.core.{ Ack, Controllable, SurgePartitionRouter }
import surge.health.HealthSignalBusTrait
import surge.internal.SurgeModel
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.config.TimeoutConfig
import surge.internal.persistence.RoutableMessage
import surge.internal.utils.DiagnosticContextFuturePropagation
import surge.kafka.streams.{ HealthCheck, HealthCheckStatus, HealthyActor, HealthyComponent }
import surge.kafka.{ KafkaPartitionShardRouterActor, PersistentActorRegionCreator }

import scala.concurrent.{ ExecutionContext, Future }
import scala.languageFeature.postfixOps

private[surge] final class SurgePartitionRouterImpl(
    config: Config,
    system: ActorSystem,
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    businessLogic: SurgeModel[_, _, _, _],
    regionCreator: PersistentActorRegionCreator[String],
    signalBus: HealthSignalBusTrait)
    extends SurgePartitionRouter
    with HealthyComponent
    with Controllable {
  implicit val executionContext: ExecutionContext = new DiagnosticContextFuturePropagation(system.dispatcher)
  private val log = LoggerFactory.getLogger(getClass)

  private val shardRouterProps = KafkaPartitionShardRouterActor.props(
    config,
    partitionTracker,
    businessLogic.partitioner,
    businessLogic.kafka.stateTopic,
    regionCreator,
    RoutableMessage.extractEntityId)(businessLogic.tracer)

  private val routerActorName = s"${businessLogic.aggregateName}RouterActor"

  private val shardRouter = system.actorOf(shardRouterProps, name = routerActorName)
  override val actorRegion: ActorRef = shardRouter

  override def start(): Future[Ack] = {
    // TODO explicit start/stop for router actor
    //implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PartitionRouter.askTimeout)
    //actorRegion.ask(ActorLifecycleManagerActor.Start).mapTo[Ack].andThen(registrationCallback())
    Future.successful(Ack())
  }

  override def stop(): Future[Ack] = {
    // TODO explicit start/stop for router actor
    //implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PartitionRouter.askTimeout)
    //actorRegion.ask(ActorLifecycleManagerActor.Stop).mapTo[Ack]
    Future.successful(Ack())
  }

  override def shutdown(): Future[Ack] = stop()

  override def restartSignalPatterns(): Seq[Pattern] = Seq(Pattern.compile("kafka.fatal.error"))

  override def restart(): Future[Ack] = {
    for {
      _ <- stop()
      started <- start()
    } yield {
      started
    }
  }

  override def healthCheck(): Future[HealthCheck] = {
    actorRegion
      .ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout * 3) // * 3 since this rolls up 2 additional health checks below
      .mapTo[HealthCheck]
      .recoverWith { case err: Throwable =>
        log.error(s"Failed to get router-actor health check", err)
        Future.successful(HealthCheck(name = "router-actor", id = s"router-actor-${actorRegion.hashCode}", status = HealthCheckStatus.DOWN))
      }
  }
}
