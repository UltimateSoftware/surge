// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import akka.actor._
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler }
import com.ultimatesoftware.kafka.{ KafkaPartitionShardRouterActor, TopicPartitionRegionCreator }
import com.ultimatesoftware.scala.core.monitoring.metrics.MetricsProvider
import org.apache.kafka.common.TopicPartition

private[streams] final class GenericAggregateActorRouter[Agg, AggIdType, Command, Event, CmdMeta](
    system: ActorSystem,
    clusterStateTrackingActor: ActorRef,
    businessLogic: KafkaStreamsCommandBusinessLogic[Agg, AggIdType, Command, Event, CmdMeta],
    metricsProvider: MetricsProvider,
    stateMetaHandler: GlobalKTableMetadataHandler,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams) {

  val actorRegion: ActorRef = {
    val shardRegionCreator = new TPRegionProps[Agg, AggIdType, Command, Event, CmdMeta](businessLogic, metricsProvider, stateMetaHandler,
      kafkaStreamsCommand)

    val shardRouterProps = KafkaPartitionShardRouterActor.props(clusterStateTrackingActor, businessLogic.partitioner, businessLogic.stateTopic,
      shardRegionCreator, GenericAggregateActor.RoutableMessage.extractEntityId)
    system.actorOf(shardRouterProps, name = "RouterActor")
  }
}

class TPRegionProps[Agg, AggIdType, Command, Event, CmdMeta](
    businessLogic: KafkaStreamsCommandBusinessLogic[Agg, AggIdType, Command, Event, CmdMeta],
    metricsProvider: MetricsProvider,
    stateMetaHandler: GlobalKTableMetadataHandler,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams) extends TopicPartitionRegionCreator {
  override def propsFromTopicPartition(topicPartition: TopicPartition): Props = {
    PartitionRegionManagementActor.props(
      assignedPartition = topicPartition,
      businessLogic = businessLogic, stateMetaHandler = stateMetaHandler,
      aggregateKafkaStreamsImpl = kafkaStreamsCommand, metricsProvider = metricsProvider)
  }
}
