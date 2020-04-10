// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.ActorSystem
import com.ultimatesoftware.akka.cluster.ActorSystemHostAwareness
import com.ultimatesoftware.kafka.KafkaConsumerStateTrackingActor
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler, KafkaStreamsPartitionTrackerActorProvider }
import play.api.libs.json.JsValue

trait KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {
  def start(): Unit // FIXME can this return an instance of the engine instead of being a unit? That way it can just be called inline
  val businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta]
  def actorSystem: ActorSystem
}

abstract class KafkaStreamsCommandImpl[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    actorSystem: ActorSystem,
    override val businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta])
  extends KafkaStreamsCommandTrait[AggId, Agg, Command, Event, CmdMeta, EvtMeta] with ActorSystemHostAwareness {

  private implicit val system: ActorSystem = actorSystem

  private val stateChangeActor = system.actorOf(KafkaConsumerStateTrackingActor.props)
  private val stateMetaHandler = new GlobalKTableMetadataHandler(businessLogic.kafka.internalMetadataTopic)
  private val kafkaStreamsImpl = new AggregateStateStoreKafkaStreams[JsValue](
    businessLogic.aggregateName,
    businessLogic.kafka.stateTopic,
    new KafkaStreamsPartitionTrackerActorProvider(stateChangeActor), stateMetaHandler, businessLogic.aggregateValidator,
    applicationHostPort)
  protected val actorRouter = new GenericAggregateActorRouter[AggId, Agg, Command, Event, CmdMeta, EvtMeta](actorSystem, stateChangeActor,
    businessLogic, businessLogic.metricsProvider, stateMetaHandler, kafkaStreamsImpl)

  def start(): Unit = {
    kafkaStreamsImpl.start()
  }
}
