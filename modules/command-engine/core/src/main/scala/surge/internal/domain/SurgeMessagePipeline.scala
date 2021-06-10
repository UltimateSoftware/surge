// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import com.typesafe.config.Config
import play.api.libs.json.JsValue
import surge.core.{ SurgePartitionRouter, SurgeProcessingTrait }
import surge.health.{ HealthSignalBusAware, HealthSignalBusTrait }
import surge.internal.SurgeModel
import surge.internal.akka.cluster.ActorSystemHostAwareness
import surge.internal.akka.kafka.{ CustomConsumerGroupRebalanceListener, KafkaConsumerPartitionAssignmentTracker, KafkaConsumerStateTrackingActor }
import surge.internal.core.SurgePartitionRouterImpl
import surge.internal.domain.SurgeMessagePipeline.PipelineControlActor
import surge.internal.health.HealthSignalStreamProvider
import surge.internal.health.supervisor.{ ShutdownComponent, Stop }
import surge.internal.persistence.PersistentActorRegionCreator
import surge.kafka.PartitionAssignments
import surge.kafka.streams.{ AggregateStateStoreKafkaStreams, HealthCheck, HealthyComponent, KafkaStreamsPartitionTrackerActorProvider, SurgeHealthCheck }

import scala.concurrent.{ ExecutionContext, Future }

object SurgeMessagePipeline {
  class PipelineControlActor[S, M, +R, E](pipeline: SurgeMessagePipeline[S, M, R, E]) extends Actor {
    override def receive: Receive = {
      case ShutdownComponent(_) =>
        pipeline.shutdown()
      case Stop => context.stop(self)
    }
  }
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

  protected implicit val system: ActorSystem = actorSystem
  protected val stateChangeActor: ActorRef = system.actorOf(KafkaConsumerStateTrackingActor.props)

  protected val partitionTracker: KafkaConsumerPartitionAssignmentTracker = new KafkaConsumerPartitionAssignmentTracker(stateChangeActor)

  protected val kafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue]

  // Get a supervised HealthSignalBus from the HealthSignalStream Provider.
  //  Intentionally do not start on init i.e. startStreamOnInit = false.  Delegate start to pipeline lifecycle.
  override val signalBus: HealthSignalBusTrait = signalStreamProvider.busWithSupervision()

  private var pipelineControlActor: ActorRef = _

  protected val cqrsRegionCreator: PersistentActorRegionCreator[M] =
    new PersistentActorRegionCreator[M](actorSystem, businessLogic, kafkaStreamsImpl, partitionTracker, businessLogic.metrics, signalBus, config = config)

  protected val actorRouter: SurgePartitionRouter = SurgePartitionRouter(actorSystem, partitionTracker, businessLogic, cqrsRegionCreator, signalBus)

  protected val surgeHealthCheck: SurgeHealthCheck = new SurgeHealthCheck(businessLogic.aggregateName, kafkaStreamsImpl, actorRouter)(ExecutionContext.global)
  protected def createPartitionRouter(): SurgePartitionRouter =
    new SurgePartitionRouterImpl(actorSystem, partitionTracker, businessLogic, cqrsRegionCreator, signalBus)
  protected def createStateStore() = new AggregateStateStoreKafkaStreams[JsValue](
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

  override def healthCheck(): Future[HealthCheck] = {
    surgeHealthCheck.healthCheck()
  }

  protected def registerRebalanceCallback(callback: PartitionAssignments => Unit): Unit = {
    system.actorOf(CustomConsumerGroupRebalanceListener.props(stateChangeActor, callback))
  }

  override def start(): Unit = {
    signalBus.signalStream().subscribe().start()

    actorRouter.start()

    pipelineControlActor = system.actorOf(Props(new PipelineControlActor(pipeline = this)))
    kafkaStreamsImpl.start()

    // Register an actorRef on behalf of the Pipeline for control.
    signalBus
      .registration(
        ref = pipelineControlActor,
        componentName = "surge-message-pipeline",
        restartSignalPatterns = restartSignalPatterns(),
        shutdownSignalPatterns = shutdownSignalPatterns())
      .invoke()
  }

  override def restart(): Unit = {
    signalBus.signalStream().unsubscribe().stop()
    kafkaStreamsImpl.restart()
    signalBus.signalStream().subscribe().start()
  }

  override def stop(): Unit = {
    // Stop router
    actorRouter.stop()
    // Stop Kafka Streams
    kafkaStreamsImpl.stop()

    // Stop Pipeline Control
    Option(pipelineControlActor).foreach(a => a ! Stop)

    // Stop Signal Stream
    signalBus.signalStream().unsubscribe().stop()
  }

  override def shutdown(): Unit = stop()
}
