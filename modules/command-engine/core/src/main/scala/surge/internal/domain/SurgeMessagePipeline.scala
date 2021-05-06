// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.internal.domain

import akka.actor.{ ActorRef, ActorSystem }
import com.typesafe.config.Config
import play.api.libs.json.JsValue
import surge.core.{ SurgePartitionRouter, SurgeProcessingTrait }
import surge.internal.SurgeModel
import surge.internal.akka.cluster.ActorSystemHostAwareness
import surge.internal.akka.kafka.KafkaConsumerStateTrackingActor
import surge.internal.persistence.PersistentActorRegionCreator
import surge.kafka.streams._

import scala.concurrent.{ ExecutionContext, Future }

/**
 * Surge message processing pipeline
 */
private[surge] abstract class SurgeMessagePipeline[S, M, +R, E](
    actorSystem: ActorSystem,
    override val businessLogic: SurgeModel[S, M, R, E],
    override val config: Config)
    extends SurgeProcessingTrait[S, M, R, E]
    with ActorSystemHostAwareness {

  protected implicit val system: ActorSystem = actorSystem
  private val cqrsRegionCreator = new PersistentActorRegionCreator[M](actorSystem, businessLogic, kafkaStreamsImpl, businessLogic.metrics, config)
  protected val actorRouter: SurgePartitionRouter
  protected val kafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue]

  protected val stateChangeActor: ActorRef = system.actorOf(KafkaConsumerStateTrackingActor.props)

  protected val surgeHealthCheck = new SurgeHealthCheck(businessLogic.aggregateName, kafkaStreamsImpl, actorRouter)(ExecutionContext.global)

  protected def createPartitionRouter() = new SurgePartitionRouter(actorSystem, stateChangeActor, businessLogic, cqrsRegionCreator)
  protected def createStateStore() = new AggregateStateStoreKafkaStreams[JsValue](
    aggregateName = businessLogic.aggregateName,
    stateTopic = businessLogic.kafka.stateTopic,
    partitionTrackerProvider = new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor),
    aggregateValidator = businessLogic.aggregateValidator,
    applicationHostPort = applicationHostPort,
    applicationId = businessLogic.kafka.streamsApplicationId,
    clientId = businessLogic.kafka.clientId,
    system = system,
    metrics = businessLogic.metrics)

  def healthCheck: Future[HealthCheck] = {
    surgeHealthCheck.healthCheck()
  }

  def start(): Unit = {
    kafkaStreamsImpl.start()
  }

  def restart(): Unit = {
    kafkaStreamsImpl.restart()
  }

  def stop(): Unit = {
    kafkaStreamsImpl.stop()
  }
}
