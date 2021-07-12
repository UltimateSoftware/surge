// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.{ ActorRef, ActorSystem }
import com.typesafe.config.Config
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.JsValue
import surge.core.{ Ack, SurgePartitionRouter, SurgeProcessingTrait }
import surge.health.{ HealthAck, HealthSignalBusAware, HealthSignalBusTrait }
import surge.internal.SurgeModel
import surge.internal.akka.cluster.ActorSystemHostAwareness
import surge.internal.akka.kafka.{ CustomConsumerGroupRebalanceListener, KafkaConsumerPartitionAssignmentTracker, KafkaConsumerStateTrackingActor }
import surge.internal.core.SurgePartitionRouterImpl
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

  private val partitionTracker: KafkaConsumerPartitionAssignmentTracker = new KafkaConsumerPartitionAssignmentTracker(stateChangeActor)

  // Get a supervised HealthSignalBus from the HealthSignalStream Provider.
  //  Intentionally do not start on init i.e. busWithSupervision(startStreamOnInit = false).  Delegate start to pipeline lifecycle.
  override val signalBus: HealthSignalBusTrait = signalStreamProvider.busWithSupervision()

  protected val kafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue] = new AggregateStateStoreKafkaStreams[JsValue](
    aggregateName = businessLogic.aggregateName,
    stateTopic = businessLogic.kafka.stateTopic,
    partitionTrackerProvider = new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor),
    aggregateValidator = businessLogic.aggregateValidator,
    applicationHostPort = applicationHostPort,
    applicationId = businessLogic.kafka.streamsApplicationId,
    clientId = businessLogic.kafka.clientId,
    system = system,
    metrics = businessLogic.metrics,
    signalBus = signalBus)

  protected val cqrsRegionCreator: PersistentActorRegionCreator[M] =
    new PersistentActorRegionCreator[M](actorSystem, businessLogic, kafkaStreamsImpl, partitionTracker, businessLogic.metrics, signalBus, config = config)

  protected val actorRouter: SurgePartitionRouter = SurgePartitionRouter(actorSystem, partitionTracker, businessLogic, cqrsRegionCreator, signalBus)

  protected val surgeHealthCheck: SurgeHealthCheck = new SurgeHealthCheck(businessLogic.aggregateName, kafkaStreamsImpl, actorRouter)(ExecutionContext.global)
  protected def createPartitionRouter(): SurgePartitionRouter =
    new SurgePartitionRouterImpl(actorSystem, partitionTracker, businessLogic, cqrsRegionCreator, signalBus)

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
      signalStreamStarted <- startSignalStream()
      actorRouterStarted <- startActorRouter(signalStreamStarted)
      kafkaStreamsStarted <- startKafkaStreams(actorRouterStarted)
    } yield {
      if (!actorRouterStarted.success) {
        for {
          _ <- stopSignalStream()
          _ <- stopActorRouter()
        } yield {
          Future.successful(HealthAck(success = false))
        }
      } else if (!kafkaStreamsStarted.success) {
        for {
          _ <- stopSignalStream()
          _ <- stopActorRouter()
          _ <- stopKafkaStreams()
        } yield {
          Future.successful(HealthAck(success = false))
        }
      }
    }

    result.onComplete(registrationCallback())
    result.map(s => s.asInstanceOf[Ack])
  }

  private def start(stopped: Ack): Future[Ack] = {
    if (stopped.success) {
      start()
    } else {
      Future.successful(HealthAck(success = false, error = Some(new RuntimeException("Unable to start SurgeMessagePipeline"))))
    }
  }

  // todo: fix; stopping and starting actor-router fails.
  override def restart(): Future[Ack] = {
    implicit val ec: ExecutionContext = system.dispatcher

    signalBus.signalStream().unsubscribe().stop()
    signalBus.signalStream().subscribe().start()
    kafkaStreamsImpl.restart()

    for {
      stopped <- stop()
      started <- start(stopped)
    } yield {
      started
    }
  }

  override def stop(): Future[Ack] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val result = for {
      signalStreamStopped <- stopSignalStream()
      //actorRouterStopped <- stopActorRouter()
      kafkaStreamsStopped <- stopKafkaStreams()
    } yield {
      val success = (signalStreamStopped.success
      //&& actorRouterStopped.success
        && kafkaStreamsStopped.success)
      HealthAck(success = success)
    }

    result
  }

  override def shutdown(): Future[Ack] = stop()

  private def startSignalStream(): Future[Ack] = Future {
    val signalStream = signalBus.signalStream()
    log.debug("Starting Health Signal Stream")
    signalStream.start()

    HealthAck(success = true)
  }(system.dispatcher)

  private def startActorRouter(signalStreamStarted: Ack): Future[Ack] = {
    if (signalStreamStarted.success) {
      actorRouter.start()
    } else {
      Future.failed(new RuntimeException("Failed to start signal stream"))
    }
  }

  private def startKafkaStreams(actorRouterStarted: Ack): Future[Ack] = {
    if (actorRouterStarted.success) {
      kafkaStreamsImpl.start()
    } else {
      Future.failed(new RuntimeException("Failed to start actor router"))
    }
  }

  private def stopActorRouter(): Future[Ack] = actorRouter.stop()

  private def stopSignalStream(): Future[Ack] = Future {
    log.debug("Stopping Health Signal Stream")
    signalBus.signalStream().unsubscribe().stop()
    HealthAck(success = true)
  }(system.dispatcher)

  private def stopKafkaStreams(): Future[Ack] = kafkaStreamsImpl.stop()

  private def registrationCallback(): Try[Any] => Unit = {
    case Success(_) =>
      val registrationResult = signalBus.register(
        control = this,
        componentName = "surge-message-pipeline",
        restartSignalPatterns = restartSignalPatterns(),
        shutdownSignalPatterns = shutdownSignalPatterns())

      registrationResult.onComplete {
        case Failure(exception) =>
          log.error("AggregateStateStore registeration failed", exception)
        case Success(done) =>
          log.debug(s"AggregateStateStore registeration succeeded - ${done.success}")
      }(system.dispatcher)
    case Failure(exception) =>
      log.error("Failed to register start so unable to register for supervision", exception)
  }
}
