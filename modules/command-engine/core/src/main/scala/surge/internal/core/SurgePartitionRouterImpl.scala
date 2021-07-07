// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.core

import java.util.regex.Pattern

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import surge.core.{ ControlAck, Controllable, SurgePartitionRouter }
import surge.health.HealthSignalBusTrait
import surge.internal.SurgeModel
import surge.internal.akka.actor.ActorLifecycleManagerActor
import surge.internal.akka.actor.ActorLifecycleManagerActor.Ack
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.config.TimeoutConfig
import surge.internal.persistence.RoutableMessage
import surge.kafka.streams.{ HealthCheck, HealthCheckStatus, HealthyActor, HealthyComponent }
import surge.kafka.{ KafkaPartitionShardRouterActor, PersistentActorRegionCreator }

import scala.concurrent.{ ExecutionContext, Future }
import scala.languageFeature.postfixOps
import scala.util.{ Failure, Success, Try }

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

  override def start(): Future[ControlAck] = {
    implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PartitionRouter.askTimeout)

    val result = actorRegion.ask(ActorLifecycleManagerActor.Start).map {
      case ack: Ack =>
        ControlAck(ack.success)
      case _ =>
        ControlAck(success = false, error = Some(new RuntimeException("Unexpected response from actor start request")))
    }

    result.onComplete(registrationHandler())
    result
  }

  override def stop(): Future[ControlAck] = {
    implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PartitionRouter.askTimeout)

    val result = actorRegion.ask(ActorLifecycleManagerActor.Stop).map {
      case ack: Ack =>
        ControlAck(ack.success)
      case _ =>
        ControlAck(success = false, error = Some(new RuntimeException("Unexpected response from actor stop request")))
    }

    result
  }

  override def shutdown(): Future[ControlAck] = stop()

  override def restartSignalPatterns(): Seq[Pattern] = Seq(Pattern.compile("kafka.fatal.error"))

  override def restart(): Future[ControlAck] = {
    for {
      stopped <- stop()
      started <- start(stopped)
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
      }(ExecutionContext.global)
  }

  private def start(stopped: ControlAck): Future[ControlAck] = {
    if (stopped.success) {
      start()
    } else {
      Future { ControlAck(success = false, error = Some(new RuntimeException("Failed to stop Surge Partition Router"))) }
    }
  }

  private def registrationHandler(): Try[Any] => Unit = {
    case Success(_) =>
      val registrationResult = signalBus.register(control = this, componentName = "router-actor", restartSignalPatterns())

      registrationResult.onComplete {
        case Failure(exception) =>
          log.error("SurgePartitionRouter registration failed", exception)
        case Success(done) =>
          log.debug(s"SurgePartitionRouter registration succeeded - ${done.success}")
      }(system.dispatcher)
    case Failure(error) =>
      log.error("Failed to register Surge Partition Router for supervision", error)
  }
}
