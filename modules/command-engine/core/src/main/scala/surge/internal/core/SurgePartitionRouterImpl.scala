// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.core

import java.util.regex.Pattern

import akka.actor._
import akka.pattern.ask
import org.slf4j.LoggerFactory
import surge.core.{ Controllable, SurgePartitionRouter }
import surge.health.HealthSignalBusTrait
import surge.internal.SurgeModel
import surge.internal.akka.actor.ActorLifecycleManagerActor
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.config.TimeoutConfig
import surge.internal.persistence.RoutableMessage
import surge.kafka.streams.{ HealthCheck, HealthCheckStatus, HealthyActor, HealthyComponent }
import surge.kafka.{ KafkaPartitionShardRouterActor, PersistentActorRegionCreator }

import scala.concurrent.{ ExecutionContext, Future }
import scala.languageFeature.postfixOps
import scala.util.{ Failure, Success }

private[surge] final class SurgePartitionRouterImpl(
    system: ActorSystem,
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    businessLogic: SurgeModel[_, _, _, _],
    regionCreator: PersistentActorRegionCreator[String],
    signalBus: HealthSignalBusTrait)
    extends SurgePartitionRouter
    with HealthyComponent
    with Controllable {
  implicit val executionContext: ExecutionContext = system.dispatcher
  private val log = LoggerFactory.getLogger(getClass)

  private val shardRouterProps = KafkaPartitionShardRouterActor.props(
    partitionTracker,
    businessLogic.partitioner,
    businessLogic.kafka.stateTopic,
    regionCreator,
    RoutableMessage.extractEntityId,
    businessLogic.tracer)

  private val lifecycleManager = system.actorOf(Props(new ActorLifecycleManagerActor(shardRouterProps, Some(s"${businessLogic.aggregateName}RouterActor"))))
  override val actorRegion: ActorRef = lifecycleManager

  override def start(): Unit = {
    actorRegion ! ActorLifecycleManagerActor.Start
    val registrationResult = signalBus.register(control = this, componentName = "router-actor", restartSignalPatterns())

    registrationResult.onComplete {
      case Failure(exception) =>
        log.error("AggregateStateStore registration failed", exception)
      case Success(done) =>
        log.debug(s"AggregateStateStore registration succeeded - ${done.success}")
    }(system.dispatcher)
  }

  override def stop(): Unit = {
    actorRegion ! ActorLifecycleManagerActor.Stop
  }

  override def shutdown(): Unit = {
    stop()
    super.shutdown()
  }

  override def restartSignalPatterns(): Seq[Pattern] = Seq(Pattern.compile("kafka.fatal.error"))

  override def restart(): Unit = {
    stop()
    start()

    super.restart()
  }

  override def healthCheck(): Future[HealthCheck] = {
    actorRegion
      .ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout * 3) // * 3 since this rolls up 2 additional health checks below
      .mapTo[HealthCheck]
      .recoverWith { case err: Throwable =>
        log.error(s"Failed to get router-actor health check", err)
        Future.successful(HealthCheck(name = "router-actor", id = s"router-actor-${actorRegion.hashCode}", status = HealthCheckStatus.DOWN))
      }(ExecutionContext.global)
  }
}
