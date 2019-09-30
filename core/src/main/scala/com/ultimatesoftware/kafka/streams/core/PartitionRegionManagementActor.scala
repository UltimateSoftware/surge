// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import akka.actor._
import com.ultimatesoftware.akka.cluster.{ EntityPropsProvider, PerShardLogicProvider, Shard }
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler }
import com.ultimatesoftware.scala.core.domain._
import com.ultimatesoftware.scala.core.monitoring.metrics.MetricsProvider
import org.apache.kafka.common.TopicPartition

class GenericAggregateActorRegionProvider[Agg, AggIdType, Command, Event, Resource, CmdMeta](
    assignedPartition: TopicPartition,
    businessLogic: KafkaStreamsCommandBusinessLogic[Agg, AggIdType, Command, Event, Resource, CmdMeta],
    stateMetaHandler: GlobalKTableMetadataHandler,
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams,
    metricsProvider: MetricsProvider) extends PerShardLogicProvider[AggIdType] {
  override def actorProvider(context: ActorContext): EntityPropsProvider[AggIdType] = {
    val kafkaProducerActor = new KafkaProducerActor(
      actorSystem = context.system,
      assignedPartition = assignedPartition,
      metricsProvider = metricsProvider,
      stateMetaHandler = stateMetaHandler,
      aggregateCommandKafkaStreams = businessLogic,
      kafkaStreamsImpl = aggregateKafkaStreamsImpl)

    val aggregateMetrics = GenericAggregateActor.createMetrics(metricsProvider, businessLogic.businessLogicAdapter.aggregateName)

    new GenericAggregateActorProvider(businessLogic.businessLogicAdapter, kafkaProducerActor, aggregateKafkaStreamsImpl, aggregateMetrics)
  }
}

class GenericAggregateActorProvider[Agg, AggIdType, Command, Event, Resource, CmdMeta](
    businessLogic: DomainBusinessLogicAdapter[Agg, AggIdType, Command, Event, Resource, CmdMeta],
    kafkaProducerActor: KafkaProducerActor[Agg, AggIdType, Event],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams,
    aggregateMetrics: GenericAggregateActor.GenericAggregateActorMetrics) extends EntityPropsProvider[AggIdType] {

  override def actorPropsById(actorId: AggIdType): Props = GenericAggregateActor.props(
    aggregateId = actorId, businessLogic = businessLogic,
    kafkaProducerActor = kafkaProducerActor, metrics = aggregateMetrics, kafkaStreamsCommand = aggregateKafkaStreamsImpl)
}

object PartitionRegionManagementActor {
  def props[Agg, AggIdType, Command, Event, Resource, CmdMeta](
    assignedPartition: TopicPartition,
    businessLogic: KafkaStreamsCommandBusinessLogic[Agg, AggIdType, Command, Event, Resource, CmdMeta],
    stateMetaHandler: GlobalKTableMetadataHandler,
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams,
    metricsProvider: MetricsProvider): Props = {

    val provider = new GenericAggregateActorRegionProvider(assignedPartition, businessLogic,
      stateMetaHandler, aggregateKafkaStreamsImpl, metricsProvider)

    Shard.props(assignedPartition.toString, provider, GenericAggregateActor.CommandEnvelope.extractEntityId)
  }
}
