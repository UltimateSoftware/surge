// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor._
import akka.pattern.ask
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import play.api.libs.json.JsValue
import surge.akka.cluster.{ EntityPropsProvider, PerShardLogicProvider, Shard }
import surge.config.TimeoutConfig
import surge.kafka.{ KafkaPartitionShardRouterActor, TopicPartitionRegionCreator }
import surge.kafka.streams.{ AggregateStateStoreKafkaStreams, HealthCheck, HealthCheckStatus, HealthyActor, HealthyComponent }
import surge.metrics.Metrics
import surge.support.Logging

import scala.concurrent.{ ExecutionContext, Future }

private[surge] final class GenericAggregateActorRouter[Agg, Command, Event](
    system: ActorSystem,
    clusterStateTrackingActor: ActorRef,
    businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event],
    metrics: Metrics,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]) extends HealthyComponent {

  private val log = LoggerFactory.getLogger(getClass)

  val actorRegion: ActorRef = {
    val shardRegionCreator = new TopicPartitionRegionCreator {
      override def propsFromTopicPartition(topicPartition: TopicPartition): Props = {
        val provider = new GenericAggregateActorRegionProvider(system, topicPartition, businessLogic,
          kafkaStreamsCommand, metrics)

        Shard.props(topicPartition.toString, provider, GenericAggregateActor.RoutableMessage.extractEntityId)
      }
    }

    val shardRouterProps = KafkaPartitionShardRouterActor.props(clusterStateTrackingActor, businessLogic.partitioner, businessLogic.kafka.stateTopic,
      shardRegionCreator, GenericAggregateActor.RoutableMessage.extractEntityId)
    val actorName = s"${businessLogic.aggregateName}RouterActor"
    system.actorOf(shardRouterProps, name = actorName)
  }

  override def healthCheck(): Future[HealthCheck] = {
    actorRegion
      .ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout * 3)
      .mapTo[HealthCheck]
      .recoverWith {
        case err: Throwable =>
          log.error(s"Failed to get router-actor health check", err)
          Future.successful(
            HealthCheck(
              name = "router-actor",
              id = s"router-actor-${actorRegion.hashCode}",
              status = HealthCheckStatus.DOWN))
      }(ExecutionContext.global)
  }
}

class GenericAggregateActorRegionProvider[Agg, Command, Event](
    system: ActorSystem,
    assignedPartition: TopicPartition,
    businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    metrics: Metrics) extends PerShardLogicProvider[String] with Logging {

  val kafkaProducerActor: KafkaProducerActor[Agg, Event] = KafkaProducerActor[Agg, Event](
    actorSystem = system,
    assignedPartition = assignedPartition,
    metrics = metrics,
    businessLogic = businessLogic,
    kStreams = aggregateKafkaStreamsImpl)

  override def actorProvider(context: ActorContext): EntityPropsProvider[String] = {
    val aggregateMetrics = GenericAggregateActor.createMetrics(metrics, businessLogic.aggregateName)

    actorId: String => GenericAggregateActor.props(aggregateId = actorId, businessLogic = businessLogic,
      kafkaProducerActor = kafkaProducerActor, metrics = aggregateMetrics, kafkaStreamsCommand = aggregateKafkaStreamsImpl)
  }

  override def onShardTerminated(): Unit = {
    log.debug("Shard for partition {} terminated, killing partition kafkaProducerActor", assignedPartition)
    kafkaProducerActor.terminate()
  }

  override def healthCheck(): Future[HealthCheck] = {
    kafkaProducerActor.healthCheck()
  }
}
