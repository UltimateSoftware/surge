// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor._
import com.ultimatesoftware.akka.cluster.{ EntityPropsProvider, PerShardLogicProvider, Shard }
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler }
import com.ultimatesoftware.scala.core.monitoring.metrics.MetricsProvider
import org.apache.kafka.common.TopicPartition
import play.api.libs.json.JsValue

class GenericAggregateActorRegionProvider[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    assignedPartition: TopicPartition,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    stateMetaHandler: GlobalKTableMetadataHandler,
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    metricsProvider: MetricsProvider) extends PerShardLogicProvider[AggId] {
  override def actorProvider(context: ActorContext): EntityPropsProvider[AggId] = {
    val kafkaProducerActor = new KafkaProducerActor(
      actorSystem = context.system,
      assignedPartition = assignedPartition,
      metricsProvider = metricsProvider,
      stateMetaHandler = stateMetaHandler,
      aggregateCommandKafkaStreams = businessLogic)

    val aggregateMetrics = GenericAggregateActor.createMetrics(metricsProvider, businessLogic.aggregateName)

    new GenericAggregateActorProvider(businessLogic, kafkaProducerActor, aggregateKafkaStreamsImpl, aggregateMetrics)
  }
}

class GenericAggregateActorProvider[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    kafkaProducerActor: KafkaProducerActor[AggId, Agg, Event],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    aggregateMetrics: GenericAggregateActor.GenericAggregateActorMetrics) extends EntityPropsProvider[AggId] {

  override def actorPropsById(actorId: AggId): Props = GenericAggregateActor.props(
    aggregateId = actorId, businessLogic = businessLogic,
    kafkaProducerActor = kafkaProducerActor, metrics = aggregateMetrics, kafkaStreamsCommand = aggregateKafkaStreamsImpl)
}

object PartitionRegionManagementActor {
  def props[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    assignedPartition: TopicPartition,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    stateMetaHandler: GlobalKTableMetadataHandler,
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    metricsProvider: MetricsProvider): Props = {

    val provider = new GenericAggregateActorRegionProvider(assignedPartition, businessLogic,
      stateMetaHandler, aggregateKafkaStreamsImpl, metricsProvider)

    Shard.props(assignedPartition.toString, provider, GenericAggregateActor.RoutableMessage.extractEntityId)
  }
}
