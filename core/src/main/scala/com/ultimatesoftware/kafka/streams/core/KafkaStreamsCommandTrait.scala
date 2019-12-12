// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.ActorSystem
import com.ultimatesoftware.akka.cluster.RemoteAddressExtension
import com.ultimatesoftware.kafka.KafkaConsumerStateTrackingActor
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler, KafkaStreamsPartitionTrackerActorProvider }
import play.api.libs.json.JsValue

trait KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {
  def start(): Unit // FIXME can this return an instance of the engine instead of being a unit? That way it can just be called inline
  val businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]
  def actorSystem: ActorSystem
}

trait KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta] extends KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {
  val actorSystem: ActorSystem
  private implicit val system: ActorSystem = actorSystem

  private val akkaAddress = RemoteAddressExtension(system).address
  private val akkaHost = akkaAddress.host.getOrElse(throw new RuntimeException("Unable to determine hostname of current Akka actor system"))
  private val akkaPort = akkaAddress.port.getOrElse(throw new RuntimeException("Unable to determine port of current Akka actor system"))
  private val applicationHostPort = s"$akkaHost:$akkaPort"

  private val stateChangeActor = system.actorOf(KafkaConsumerStateTrackingActor.props)
  private val stateMetaHandler = new GlobalKTableMetadataHandler(businessLogic.kafka.internalMetadataTopic)
  private val kafkaStreamsImpl = new AggregateStateStoreKafkaStreams[JsValue](
    businessLogic.aggregateName,
    businessLogic.kafka.stateTopic, new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor), stateMetaHandler, businessLogic.aggregateValidator,
    Some(applicationHostPort))
  protected val actorRouter = new GenericAggregateActorRouter[AggId, Agg, Command, Event, CmdMeta, EvtMeta](actorSystem, stateChangeActor,
    businessLogic, businessLogic.metricsProvider, stateMetaHandler, kafkaStreamsImpl)

  def start(): Unit = {
    kafkaStreamsImpl.start()
  }
}
