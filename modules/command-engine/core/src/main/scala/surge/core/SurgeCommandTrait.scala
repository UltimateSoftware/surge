// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.ActorSystem
import play.api.libs.json.JsValue
import surge.internal.akka.cluster.ActorSystemHostAwareness
import surge.internal.akka.kafka.KafkaConsumerStateTrackingActor
import surge.internal.persistence.cqrs.CQRSPersistentActorRegionCreator
import surge.kafka.streams._

import scala.concurrent.{ ExecutionContext, Future }

trait SurgeCommandTrait[Agg, Command, Event] {
  def start(): Unit
  def restart(): Unit
  def stop(): Unit
  val businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event]
  def actorSystem: ActorSystem
}

abstract class SurgeCommandImpl[Agg, Command, Event](
    actorSystem: ActorSystem,
    override val businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event])
  extends SurgeCommandTrait[Agg, Command, Event] with ActorSystemHostAwareness {

  private implicit val system: ActorSystem = actorSystem

  private val stateChangeActor = system.actorOf(KafkaConsumerStateTrackingActor.props)
  private val kafkaStreamsImpl = new AggregateStateStoreKafkaStreams[JsValue](
    aggregateName = businessLogic.aggregateName,
    stateTopic = businessLogic.kafka.stateTopic,
    partitionTrackerProvider = new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor),
    aggregateValidator = businessLogic.aggregateValidator,
    applicationHostPort = applicationHostPort,
    consumerGroupName = businessLogic.kafka.consumerGroup,
    clientId = businessLogic.kafka.clientId,
    system = system,
    metrics = businessLogic.metrics)
  private val cqrsRegionCreator = new CQRSPersistentActorRegionCreator[Agg, Command, Event](actorSystem, businessLogic,
    kafkaStreamsImpl, businessLogic.metrics)
  protected val actorRouter = new SurgePartitionRouter[Agg, Command, Event](actorSystem, stateChangeActor,
    businessLogic, cqrsRegionCreator)

  protected val surgeHealthCheck = new SurgeHealthCheck(
    businessLogic.aggregateName,
    kafkaStreamsImpl,
    actorRouter)(ExecutionContext.global)

  def healthCheck(): Future[HealthCheck] = {
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
