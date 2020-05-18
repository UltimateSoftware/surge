// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.ActorSystem
import com.ultimatesoftware.akka.cluster.ActorSystemHostAwareness
import com.ultimatesoftware.kafka.KafkaConsumerStateTrackingActor
import com.ultimatesoftware.kafka.streams._
import org.slf4j.LoggerFactory
import play.api.libs.json.JsValue

import scala.concurrent.{ ExecutionContext, Future }

trait KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {
  def start(): Unit // FIXME can this return an instance of the engine instead of being a unit? That way it can just be called inline
  def stop(): Unit
  val businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]
  def actorSystem: ActorSystem
}

abstract class KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    actorSystem: ActorSystem,
    override val businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta])
  extends KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] with ActorSystemHostAwareness {

  private val log = LoggerFactory.getLogger(getClass)
  private implicit val system: ActorSystem = actorSystem

  private val stateChangeActor = system.actorOf(KafkaConsumerStateTrackingActor.props)
  private val stateMetaHandler = new GlobalKTableMetadataHandler(businessLogic.kafka.internalMetadataTopic, businessLogic.consumerGroup)
  private val kafkaStreamsImpl = new AggregateStateStoreKafkaStreams[JsValue](
    businessLogic.aggregateName,
    businessLogic.kafka.stateTopic,
    new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor), stateMetaHandler, businessLogic.aggregateValidator,
    applicationHostPort,
    businessLogic.consumerGroup)
  protected val actorRouter = new GenericAggregateActorRouter[AggId, Agg, Command, Event, CmdMeta, EvtMeta](actorSystem, stateChangeActor,
    businessLogic, businessLogic.metricsProvider, stateMetaHandler, kafkaStreamsImpl)

  protected val surgeHealthCheck = new SurgeHealthCheck(
    businessLogic.aggregateName,
    stateMetaHandler,
    kafkaStreamsImpl,
    actorRouter)(ExecutionContext.global)

  def healthCheck(): Future[HealthCheck] = {
    surgeHealthCheck.healthCheck()
  }

  def start(): Unit = {
    kafkaStreamsImpl.start()
  }

  /**
   * WARNING: Don't use this except for shutting down between tests.  The engine can not be restarted properly after shutting down via this method and
   * the full app must be restarted.  Restarting may be supported in future versions of Surge, but for now it will not work.
   */
  def stop(): Unit = {
    log.warn("Shutting down the Surge engine! The engine should NOT be restarted or it may not work properly. " +
      "This method should only be used to clean up between integration tests.")
    kafkaStreamsImpl.stop()
  }
}
