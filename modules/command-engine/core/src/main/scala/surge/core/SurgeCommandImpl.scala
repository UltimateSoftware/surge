// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.ActorSystem
import com.typesafe.config.Config
import play.api.libs.json.JsValue
import surge.internal.akka.cluster.ActorSystemHostAwareness
import surge.internal.akka.kafka.KafkaConsumerStateTrackingActor
import surge.internal.persistence.PersistentActorRegionCreator
import surge.kafka.streams._

import scala.concurrent.{ ExecutionContext, Future }

abstract class SurgeCommandImpl[Agg, Command, +Rej, Event](
    actorSystem: ActorSystem,
    override val businessLogic: SurgeCommandModel[Agg, Command, Rej, Event],
    override val config: Config)
    extends SurgeProcessingTrait[Agg, Command, Rej, Event]
    with ActorSystemHostAwareness {

  private implicit val system: ActorSystem = actorSystem

  private val stateChangeActor = system.actorOf(KafkaConsumerStateTrackingActor.props)
  private val kafkaStreamsImpl = new AggregateStateStoreKafkaStreams[JsValue](
    aggregateName = businessLogic.aggregateName,
    stateTopic = businessLogic.kafka.stateTopic,
    partitionTrackerProvider = new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor),
    aggregateValidator = businessLogic.aggregateValidator,
    applicationHostPort = applicationHostPort,
    applicationId = businessLogic.kafka.streamsApplicationId,
    clientId = businessLogic.kafka.clientId,
    system = system,
    metrics = businessLogic.metrics)

  private val cqrsRegionCreator = new PersistentActorRegionCreator[Command](actorSystem, businessLogic, kafkaStreamsImpl, businessLogic.metrics, config)
  protected val actorRouter = new SurgePartitionRouter(actorSystem, stateChangeActor, businessLogic, cqrsRegionCreator)

  protected val surgeHealthCheck = new SurgeHealthCheck(businessLogic.aggregateName, kafkaStreamsImpl, actorRouter)(ExecutionContext.global)

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
