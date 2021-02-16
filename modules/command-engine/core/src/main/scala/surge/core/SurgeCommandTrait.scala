// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.ActorSystem
import play.api.libs.json.JsValue
import surge.akka.cluster.ActorSystemHostAwareness
import surge.kafka.KafkaConsumerStateTrackingActor
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
    businessLogic.aggregateName,
    businessLogic.kafka.stateTopic,
    new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor),
    businessLogic.aggregateValidator,
    applicationHostPort,
    businessLogic.consumerGroup,
    system)
  protected val actorRouter = new GenericAggregateActorRouter[Agg, Command, Event](actorSystem, stateChangeActor,
    businessLogic, businessLogic.metrics, kafkaStreamsImpl)

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
