// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.ActorSystem
import com.ultimatesoftware.akka.cluster.ActorSystemHostAwareness
import com.ultimatesoftware.kafka.KafkaConsumerStateTrackingActor
import com.ultimatesoftware.kafka.streams._
import play.api.libs.json.JsValue

import scala.concurrent.{ ExecutionContext, Future }

trait KafkaStreamsCommandTrait[Agg, Command, Event] {
  def start(): Unit
  def restart(): Unit
  def stop(): Unit
  val businessLogic: KafkaStreamsCommandBusinessLogic[Agg, Command, Event]
  def actorSystem: ActorSystem
}

abstract class KafkaStreamsCommandImpl[Agg, Command, Event](
    actorSystem: ActorSystem,
    override val businessLogic: KafkaStreamsCommandBusinessLogic[Agg, Command, Event])
  extends KafkaStreamsCommandTrait[Agg, Command, Event] with ActorSystemHostAwareness {

  private implicit val system: ActorSystem = actorSystem

  private val stateChangeActor = system.actorOf(KafkaConsumerStateTrackingActor.props)
  private val stateMetaHandler = new KafkaPartitionMetadataHandlerImpl(system)
  private val kafkaStreamsImpl = new AggregateStateStoreKafkaStreams[JsValue](
    businessLogic.aggregateName,
    businessLogic.kafka.stateTopic,
    new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor),
    stateMetaHandler,
    businessLogic.aggregateValidator,
    applicationHostPort,
    businessLogic.consumerGroup,
    system)
  protected val actorRouter = new GenericAggregateActorRouter[Agg, Command, Event](actorSystem, stateChangeActor,
    businessLogic, businessLogic.metricsProvider, kafkaStreamsImpl)

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
