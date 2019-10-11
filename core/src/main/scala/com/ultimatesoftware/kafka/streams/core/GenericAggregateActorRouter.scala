// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import akka.actor._
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler }
import com.ultimatesoftware.kafka.{ KafkaPartitionShardRouterActor, TopicPartitionRegionCreator }
import com.ultimatesoftware.scala.core.monitoring.metrics.MetricsProvider
import org.apache.kafka.common.TopicPartition
import play.api.libs.json.JsValue

private[streams] final class GenericAggregateActorRouter[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    system: ActorSystem,
    clusterStateTrackingActor: ActorRef,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    metricsProvider: MetricsProvider,
    stateMetaHandler: GlobalKTableMetadataHandler,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]) {

  val actorRegion: ActorRef = {
    val shardRegionCreator = new TPRegionProps[AggId, Agg, Command, Event, CmdMeta, EvtMeta](businessLogic, metricsProvider, stateMetaHandler,
      kafkaStreamsCommand)

    val shardRouterProps = KafkaPartitionShardRouterActor.props(clusterStateTrackingActor, businessLogic.partitioner, businessLogic.kafka.stateTopic,
      shardRegionCreator, GenericAggregateActor.RoutableMessage.extractEntityId)
    system.actorOf(shardRouterProps, name = "RouterActor")
  }
}

class TPRegionProps[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    metricsProvider: MetricsProvider,
    stateMetaHandler: GlobalKTableMetadataHandler,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]) extends TopicPartitionRegionCreator {
  override def propsFromTopicPartition(topicPartition: TopicPartition): Props = {
    PartitionRegionManagementActor.props(
      assignedPartition = topicPartition,
      businessLogic = businessLogic, stateMetaHandler = stateMetaHandler,
      aggregateKafkaStreamsImpl = kafkaStreamsCommand, metricsProvider = metricsProvider)
  }
}
