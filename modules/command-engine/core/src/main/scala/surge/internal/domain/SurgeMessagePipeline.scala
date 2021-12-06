// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.{ ActorRef, ActorSystem }
import akka.cluster.Cluster
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import com.typesafe.config.Config
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.JsValue
import surge.core.{ Ack, KafkaProducerActor, SurgePartitionRouter, SurgeProcessingTrait }
import surge.health.{ HealthSignalBusAware, HealthSignalBusTrait }
import surge.internal.SurgeModel
import surge.internal.akka.cluster.ActorSystemHostAwareness
import surge.internal.akka.kafka.{ CustomConsumerGroupRebalanceListener, KafkaConsumerPartitionAssignmentTracker, KafkaConsumerStateTrackingActor }
import surge.internal.health.HealthSignalStreamProvider
import surge.internal.kafka.KafkaClusterShardingRebalanceListener
import surge.internal.persistence.PersistentActorRegionCreator
import surge.kafka.PartitionAssignments
import surge.kafka.streams._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object SurgeMessagePipeline {
  val log: Logger = LoggerFactory.getLogger(getClass)
  var surgeEngineStatus: SurgeEngineStatus = SurgeEngineStatus.Stopped
}

/**
 * Surge message processing pipeline
 */
private[surge] abstract class SurgeMessagePipeline[S, M, +R, E](
    actorSystem: ActorSystem,
    override val businessLogic: SurgeModel[S, M, R, E],
    val signalStreamProvider: HealthSignalStreamProvider,
    override val config: Config)
    extends SurgeProcessingTrait[S, M, R, E]
    with HealthyComponent
    with HealthSignalBusAware
    with ActorSystemHostAwareness {

  import SurgeMessagePipeline._
  import system.dispatcher
  protected implicit val system: ActorSystem = actorSystem
  protected val stateChangeActor: ActorRef = system.actorOf(KafkaConsumerStateTrackingActor.props, "state-change-actor")

  private val isAkkaClusterEnabled: Boolean = config.getBoolean("surge.feature-flags.experimental.enable-akka-cluster")

  private val partitionTracker: KafkaConsumerPartitionAssignmentTracker = new KafkaConsumerPartitionAssignmentTracker(stateChangeActor)

  // Get a HealthSignalBus from the HealthSignalStream Provider.
  //  Intentionally do not start on init i.e. surge.health.bus.stream.start-on-init = false.
  //  Delegate start to pipeline lifecycle.
  override val signalBus: HealthSignalBusTrait = signalStreamProvider.bus()

  protected val kafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue] = new AggregateStateStoreKafkaStreams[JsValue](
    aggregateName = businessLogic.aggregateName,
    stateTopic = businessLogic.kafka.stateTopic,
    partitionTrackerProvider = new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor),
    applicationHostPort = applicationHostPort,
    applicationId = businessLogic.kafka.streamsApplicationId,
    clientId = businessLogic.kafka.clientId,
    system = system,
    metrics = businessLogic.metrics,
    signalBus = signalBus,
    config = config)

  protected val cqrsRegionCreator: PersistentActorRegionCreator[M] =
    new PersistentActorRegionCreator[M](actorSystem, businessLogic, kafkaStreamsImpl, partitionTracker, businessLogic.metrics, signalBus, config = config)

  protected val actorRouter: SurgePartitionRouter =
    SurgePartitionRouter(config, actorSystem, partitionTracker, businessLogic, kafkaStreamsImpl, cqrsRegionCreator, signalBus, isAkkaClusterEnabled)

  protected val surgeHealthCheck: SurgeHealthCheck = new SurgeHealthCheck(businessLogic.aggregateName, kafkaStreamsImpl, actorRouter)(ExecutionContext.global)

  override def healthCheck(): Future[HealthCheck] = {
    surgeHealthCheck.healthCheck()
  }

  protected def registerRebalanceCallback(callback: PartitionAssignments => Unit): Unit = {
    system.actorOf(CustomConsumerGroupRebalanceListener.props(stateChangeActor, callback))
  }

  override def start(): Future[Ack] = {
    val result = for {
      _ <- startSignalStream()
      _ <- startClusterManagementAndRebalanceListener()
      _ <- actorRouter.start()
      allStarted <- kafkaStreamsImpl.start()
    } yield {
      surgeEngineStatus = SurgeEngineStatus.Running
      log.info(s"surge engine status: $surgeEngineStatus")
      allStarted
    }

    result.andThen(registrationCallback())
    result
  }

  override def restart(): Future[Ack] = {
    val result = for {
      _ <- stop()
      started <- start()
    } yield {
      started
    }

    result
  }

  override def stop(): Future[Ack] = {
    val result = for {
      _ <- stopSignalStream()
      _ <- actorRouter.stop()
      allStopped <- kafkaStreamsImpl.stop()
    } yield {
      surgeEngineStatus = SurgeEngineStatus.Stopped
      log.info(s"surge engine status: $surgeEngineStatus")
      allStopped
    }

    result.andThen(unRegistrationCallback())
  }

  override def shutdown(): Future[Ack] = stop()

  private def startClusterManagementAndRebalanceListener(): Future[Unit] = {
    if (isAkkaClusterEnabled) {
      Cluster.get(system)
      ClusterBootstrap(system).start()
      for {
        _ <- AkkaManagement(system).start()
        allStarted <- startKafkaClusterRebalanceListener()
      } yield allStarted
    } else {
      Future.unit
    }
  }

  private def startKafkaClusterRebalanceListener(): Future[ActorRef] = Future.successful {
    val partitionToKafkaProducerActor = KafkaProducerActor.createFromPartitionNumber(
      actorSystem = system,
      metrics = businessLogic.metrics,
      businessLogic = businessLogic,
      partitionTracker = partitionTracker,
      kStreams = kafkaStreamsImpl,
      signalBus = signalBus,
      config = config)

    system.actorOf(
      KafkaClusterShardingRebalanceListener
        .props(stateChangeActor, partitionToKafkaProducerActor, businessLogic.kafka.stateTopic.name, businessLogic.kafka.streamsApplicationId))
  }

  private def startSignalStream(): Future[Ack] = {
    val signalStream = signalBus.signalStream()
    log.debug("Starting Health Signal Stream")
    signalStream.start()

    Future.successful[Ack](Ack())
  }

  private def stopSignalStream(): Future[Ack] = {
    log.debug("Stopping Health Signal Stream")
    signalBus.signalStream().unsubscribe().stop()
    Future.successful[Ack](Ack())
  }

  private def unRegistrationCallback(): PartialFunction[Try[Ack], Unit] = {
    case Success(_) =>
      unregisterWithSupervisor()
    case Failure(exception) =>
      log.error("Failed to stop so unable to unregister from supervision", exception)
  }

  private def registrationCallback(): PartialFunction[Try[Ack], Unit] = {
    case Success(_) =>
      registerWithSupervisor()
    case Failure(exception) =>
      log.error("Failed to start so unable to register for supervision", exception)
  }

  /**
   * Register for Supervision via HealthSignalBus
   */
  private def registerWithSupervisor(): Unit = {
    signalBus
      .register(
        control = this,
        componentName = "surge-message-pipeline",
        restartSignalPatterns = restartSignalPatterns(),
        shutdownSignalPatterns = shutdownSignalPatterns())
      .onComplete {
        case Failure(exception) =>
          log.error(s"$getClass registeration failed", exception)
        case Success(_) =>
          log.debug(s"$getClass registeration succeeded")
      }(system.dispatcher)
  }

  /**
   * Unregister for Supervision via HealthSignalBus
   */
  private def unregisterWithSupervisor(): Unit = {
    signalBus
      .unregister(control = this, componentName = "surge-message-pipeline")
      .onComplete {
        case Failure(exception) =>
          log.error(s"$getClass registration failed", exception)
        case Success(_) =>
          log.debug(s"$getClass registration succeeded")
      }(system.dispatcher)
  }
}
