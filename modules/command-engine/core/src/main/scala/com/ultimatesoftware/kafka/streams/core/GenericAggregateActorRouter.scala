// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor._
import com.ultimatesoftware.akka.cluster.{ EntityPropsProvider, PerShardLogicProvider, Shard }
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler, HealthCheck, HealthCheckStatus, HealthyActor, HealthyComponent }
import com.ultimatesoftware.kafka.{ KafkaPartitionShardRouterActor, TopicPartitionRegionCreator }
import com.ultimatesoftware.scala.core.monitoring.metrics.MetricsProvider
import org.apache.kafka.common.TopicPartition
import play.api.libs.json.JsValue
import akka.pattern.ask
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

private[streams] final class GenericAggregateActorRouter[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    system: ActorSystem,
    clusterStateTrackingActor: ActorRef,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    metricsProvider: MetricsProvider,
    stateMetaHandler: GlobalKTableMetadataHandler,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]) extends HealthyComponent {

  private val log = LoggerFactory.getLogger(getClass)

  val actorRegion: ActorRef = {
    val shardRegionCreator = new TopicPartitionRegionCreator {
      override def propsFromTopicPartition(topicPartition: TopicPartition): Props = {
        val kafkaProducerActor = new KafkaProducerActor[AggId, Agg, Event, EvtMeta](
          actorSystem = system,
          assignedPartition = topicPartition,
          metricsProvider = metricsProvider,
          stateMetaHandler = stateMetaHandler,
          aggregateCommandKafkaStreams = businessLogic)
        val provider = new GenericAggregateActorRegionProvider(topicPartition, businessLogic,
          stateMetaHandler, kafkaStreamsCommand, metricsProvider, kafkaProducerActor)

        Shard.props[AggId, Agg, Event, EvtMeta](topicPartition.toString, provider, GenericAggregateActor.RoutableMessage.extractEntityId, kafkaProducerActor)
      }
    }

    val shardRouterProps = KafkaPartitionShardRouterActor.props(clusterStateTrackingActor, businessLogic.partitioner, businessLogic.kafka.stateTopic,
      shardRegionCreator, GenericAggregateActor.RoutableMessage.extractEntityId)
    system.actorOf(shardRouterProps, name = "RouterActor")
  }

  override def healthCheck(): Future[HealthCheck] = {
    actorRegion
      .ask(HealthyActor.GetHealth)(3.seconds)
      .mapTo[HealthCheck]
      .recoverWith {
        case err: Throwable ⇒
          log.error(s"Failed to get router-actor health check ${err.getMessage}")
          Future.successful(
            HealthCheck(
              name = "router-actor",
              id = "set-me",
              status = HealthCheckStatus.DOWN))
      }(ExecutionContext.global)
  }
}

class GenericAggregateActorRegionProvider[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    assignedPartition: TopicPartition,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    stateMetaHandler: GlobalKTableMetadataHandler,
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    metricsProvider: MetricsProvider,
    aggregateProducerActor: KafkaProducerActor[AggId, Agg, Event, EvtMeta]) extends PerShardLogicProvider[AggId] {

  override def actorProvider(context: ActorContext): EntityPropsProvider[AggId] = {

    val aggregateMetrics = GenericAggregateActor.createMetrics(metricsProvider, businessLogic.aggregateName)

    actorId: AggId ⇒ GenericAggregateActor.props(aggregateId = actorId, businessLogic = businessLogic,
      kafkaProducerActor = aggregateProducerActor, metrics = aggregateMetrics, kafkaStreamsCommand = aggregateKafkaStreamsImpl)
  }
}
