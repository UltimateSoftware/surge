// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.KafkaConsumerStateTrackingActor
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler, KafkaStreamsPartitionTrackerActorProvider }
import play.api.libs.json.JsValue

trait KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {
  def start(): Unit
  val businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]
  val actorSystem: ActorSystem
}

trait KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta] extends KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {
  protected implicit val system: ActorSystem = actorSystem

  private val config = ConfigFactory.load()
  private val akkaHost = config.getString("akka.remote.artery.canonical.hostname")
  private val akkaPort = config.getInt("akka.remote.artery.canonical.port")
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
