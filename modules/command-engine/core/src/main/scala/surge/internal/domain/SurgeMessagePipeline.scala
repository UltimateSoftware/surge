// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.{ ActorRef, ActorSystem }
import akka.cluster.Cluster
import akka.management.scaladsl.AkkaManagement
import com.typesafe.config.Config
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.JsValue
import surge.core.{ Ack, SurgePartitionRouter, SurgeProcessingTrait }
import surge.health.{ HealthSignalBusAware, HealthSignalBusTrait }
import surge.internal.SurgeModel
import surge.internal.akka.cluster.ActorSystemHostAwareness
import surge.internal.akka.kafka.{
  CustomConsumerGroupRebalanceListener,
  KafkaClusterShardingRebalanceListener,
  KafkaConsumerPartitionAssignmentTracker,
  KafkaConsumerStateTrackingActor
}
import surge.internal.health.HealthSignalStreamProvider
import surge.internal.persistence.PersistentActorRegionCreator
import surge.kafka.PartitionAssignments
import surge.kafka.streams._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object SurgeMessagePipeline {
  val log: Logger = LoggerFactory.getLogger(getClass)
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
  protected implicit val system: ActorSystem = actorSystem
  protected val stateChangeActor: ActorRef = system.actorOf(KafkaConsumerStateTrackingActor.props)

  protected val cluster: Cluster = Cluster.get(system)
  AkkaManagement(system).start()

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
    SurgePartitionRouter(config, actorSystem, partitionTracker, businessLogic, kafkaStreamsImpl, cqrsRegionCreator, signalBus)

  protected val surgeHealthCheck: SurgeHealthCheck = new SurgeHealthCheck(businessLogic.aggregateName, kafkaStreamsImpl, actorRouter)(ExecutionContext.global)

  override def healthCheck(): Future[HealthCheck] = {
    surgeHealthCheck.healthCheck()
  }

  protected def registerRebalanceCallback(callback: PartitionAssignments => Unit): Unit = {
    system.actorOf(CustomConsumerGroupRebalanceListener.props(stateChangeActor, callback))
  }

  // todo: chain the component starts together; if one fails they all fail and we rollback and
  //  stop any that have started. if all pass, we register..
  override def start(): Future[Ack] = {
    implicit val ec: ExecutionContext = system.dispatcher
    val result = for {
      _ <- startSignalStream()
      _ <- actorRouter.start()
      _ <- startKafkaClusterRebalanceListener()
      allStarted <- kafkaStreamsImpl.start()
    } yield {
      allStarted
    }

    result.andThen(registrationCallback())
    result
  }

  override def restart(): Future[Ack] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val result = for {
      _ <- stop()
      started <- start()
    } yield {
      started
    }

    result
  }

  override def stop(): Future[Ack] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val result = for {
      _ <- stopSignalStream()
      _ <- actorRouter.stop()
      allStopped <- kafkaStreamsImpl.stop()
    } yield {
      allStopped
    }

    result
  }

  override def shutdown(): Future[Ack] = stop()

  private def startKafkaClusterRebalanceListener(): Future[ActorRef] = {
    Future.successful(
      system.actorOf(
        KafkaClusterShardingRebalanceListener.props(stateChangeActor, businessLogic.kafka.stateTopic.name, businessLogic.kafka.streamsApplicationId)))
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

  private def registrationCallback(): PartialFunction[Try[Ack], Unit] = {

    case Success(_) =>
      val registrationResult = signalBus.register(
        control = this,
        componentName = "surge-message-pipeline",
        restartSignalPatterns = restartSignalPatterns(),
        shutdownSignalPatterns = shutdownSignalPatterns())

      registrationResult.onComplete {
        case Failure(exception) =>
          log.error("AggregateStateStore registeration failed", exception)
        case Success(_) =>
          log.debug(s"AggregateStateStore registeration succeeded")
      }(system.dispatcher)
    case Failure(exception) =>
      log.error("Failed to register start so unable to register for supervision", exception)
  }
}
