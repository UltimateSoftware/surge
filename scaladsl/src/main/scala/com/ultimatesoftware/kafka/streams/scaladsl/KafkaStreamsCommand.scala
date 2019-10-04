// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.KafkaConsumerStateTrackingActor
import com.ultimatesoftware.kafka.streams.KafkaStreamsPartitionTrackerActorProvider
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler }
import com.ultimatesoftware.kafka.streams.core._

object KafkaStreamsCommand {
  def apply[Agg, AggIdType, Command, Event, Resource, CmdMeta](
    businessLogic: KafkaStreamsCommandBusinessLogic[Agg, AggIdType, Command, Event, CmdMeta]): KafkaStreamsCommand[Agg, AggIdType, Command, Event, CmdMeta] = {

    val actorSystem = ActorSystem(s"${businessLogic.domainBusinessLogicAdapter.aggregateName}ActorSystem")
    new KafkaStreamsCommand(actorSystem, businessLogic)
  }
}

class KafkaStreamsCommand[Agg, AggIdType, Command, Event, CmdMeta] private (
    val actorSystem: ActorSystem,
    businessLogic: KafkaStreamsCommandBusinessLogic[Agg, AggIdType, Command, Event, CmdMeta]) {

  private implicit val system: ActorSystem = actorSystem

  private val config = ConfigFactory.load()
  private val akkaHost = config.getString("akka.remote.netty.tcp.hostname")
  private val akkaPort = config.getInt("akka.remote.netty.tcp.port")
  private val applicationHostPort = s"$akkaHost:$akkaPort"

  private val coreKafkaStreams = businessLogic.toCore
  private val stateChangeActor = system.actorOf(KafkaConsumerStateTrackingActor.props)
  private val stateMetaHandler = new GlobalKTableMetadataHandler(businessLogic.internalMetadataTopic)
  private val kafkaStreamsImpl = new AggregateStateStoreKafkaStreams(
    coreKafkaStreams.businessLogicAdapter.aggregateName,
    coreKafkaStreams.stateTopic, new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor), stateMetaHandler, Some(applicationHostPort))
  private val actorRouter = new GenericAggregateActorRouter[Agg, AggIdType, Command, Event, CmdMeta](actorSystem, stateChangeActor,
    coreKafkaStreams, businessLogic.metricsProvider, stateMetaHandler, kafkaStreamsImpl)

  def aggregateFor(aggregateId: AggIdType): AggregateRef[AggIdType, Agg, Command, CmdMeta] = {
    new AggregateRef(aggregateId, actorRouter.actorRegion, system)
  }

  def start(): Unit = {
    kafkaStreamsImpl.start()
  }
}

