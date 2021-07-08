// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.{ ActorRef, ActorSystem }
import com.typesafe.config.Config
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.JsValue
import surge.core.{ ControlAck, SurgePartitionRouter, SurgeProcessingTrait }
import surge.health.{ HealthSignalBusAware, HealthSignalBusTrait }
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
  override def start(): Future[ControlAck] = {
    implicit val ec: ExecutionContext = system.dispatcher
    val result: Future[Any] = for {
      signalStreamStarted <- startSignalStream()
      actorRouterStarted <- startActorRouter(signalStreamStarted)
      kafkaStreamsStarted <- startKafkaStreams(actorRouterStarted)
    } yield {
      if (!actorRouterStarted.success) {
        for {
          signalStreamStopped <- stopSignalStream()
          actorRouterStopped <- stopActorRouter()
        } yield {
          Future {
            ControlAck(
              success = false,
              details = Map[String, ControlAck](elems = "signalStreamStopped" -> signalStreamStopped, "actorRouterStopped" -> actorRouterStopped))
          }
        }
      } else if (!kafkaStreamsStarted.success) {
        for {
          signalStreamStopped <- stopSignalStream()
          actorRouterStopped <- stopActorRouter()
          kafkaStreamsStopped <- stopKafkaStreams()
        } yield {
          Future {
            ControlAck(
              success = false,
              details = Map[String, ControlAck](
                elems = "signalStreamStopped" -> signalStreamStopped,
                "actorRouterStopped" -> actorRouterStopped,
                "kafkaStreamsStopped" -> kafkaStreamsStopped))
          }
        }
      }
    }

    result.onComplete(registrationCallback())
    result.map(s => s.asInstanceOf[ControlAck])
  }

  private def start(stopped: ControlAck): Future[ControlAck] = {
    if (stopped.success) {
      start()
    } else {
      Future.successful(ControlAck(success = false, error = Some(new RuntimeException("Unable to start SurgeMessagePipeline"))))
    }
  }

  // todo: fix; stopping and starting actor-router fails.
  override def restart(): Future[ControlAck] = {
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

  override def stop(): Future[ControlAck] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val result = for {
      stoppedSignalStreams <- stopSignalStream()
      _ <- stopActorRouter()
      _ <- stopKafkaStreams()
    } yield {
      stoppedSignalStreams
    }

    result
  }

  override def shutdown(): Future[ControlAck] = stop()

  private def startSignalStream(): Future[ControlAck] = Future {
    val signalStream = signalBus.signalStream()
    log.debug("Starting Health Signal Stream")
    signalStream.start()

    ControlAck(success = true)
  }(system.dispatcher)

  private def startActorRouter(signalStreamStarted: ControlAck): Future[ControlAck] = {
    if (signalStreamStarted.success) {
      actorRouter.start()
    } else {
      Future.failed(new RuntimeException("Failed to start signal stream"))
    }
  }

  private def startKafkaStreams(actorRouterStarted: ControlAck): Future[ControlAck] = {
    if (actorRouterStarted.success) {
      kafkaStreamsImpl.start()
    } else {
      Future.failed(new RuntimeException("Failed to start actor router"))
    }
  }

  private def stopActorRouter(): Future[ControlAck] = actorRouter.stop()

  private def stopSignalStream(): Future[ControlAck] = Future {
    log.debug("Stopping Health Signal Stream")
    signalBus.signalStream().unsubscribe().stop()
    ControlAck(success = true)
  }(system.dispatcher)

  private def stopKafkaStreams(): Future[ControlAck] = kafkaStreamsImpl.stop()
  
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
