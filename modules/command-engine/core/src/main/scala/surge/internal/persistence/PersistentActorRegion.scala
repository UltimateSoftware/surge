// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import akka.actor.{ ActorContext, ActorSystem, Props }
import com.typesafe.config.Config
import org.apache.kafka.common.TopicPartition
import play.api.libs.json.JsValue
import surge.akka.cluster.{ EntityPropsProvider, PerShardLogicProvider }
import surge.core.KafkaProducerActor
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.persistence
import surge.internal.utils.Logging
import surge.kafka.streams.{ AggregateStateStoreKafkaStreams, HealthCheck }
import surge.kafka.{ PersistentActorRegionCreator => KafkaPersistentActorRegionCreator }
import surge.metrics.Metrics

import scala.concurrent.Future

trait PersistentActorPropsFactory[M] extends {
  def props(aggregateId: String, businessLogic: BusinessLogic, resources: PersistentEntitySharedResources): Props
}

class PersistentActorRegionCreator[M](
    system: ActorSystem,
    businessLogic: BusinessLogic,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue],
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    metrics: Metrics,
    config: Config)
    extends KafkaPersistentActorRegionCreator[String] {
  override def regionFromTopicPartition(topicPartition: TopicPartition): PerShardLogicProvider[String] =
    new PersistentActorRegion[M](system, topicPartition, businessLogic, kafkaStreamsCommand, partitionTracker, metrics, config)
}

class PersistentActorRegion[M](
    system: ActorSystem,
    assignedPartition: TopicPartition,
    businessLogic: BusinessLogic,
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    metrics: Metrics,
    val config: Config)
    extends PerShardLogicProvider[String]
    with Logging {

  private val kafkaProducerActor: KafkaProducerActor = KafkaProducerActor(
    actorSystem = system,
    assignedPartition = assignedPartition,
    metrics = metrics,
    businessLogic = businessLogic,
    kStreams = aggregateKafkaStreamsImpl,
    partitionTracker = partitionTracker)

  override def onShardTerminated(): Unit = {
    log.debug("Shard for partition {} terminated, killing partition kafkaProducerActor", assignedPartition)
    kafkaProducerActor.terminate()
  }

  override def healthCheck(): Future[HealthCheck] = {
    kafkaProducerActor.healthCheck()
  }

  override def actorProvider(context: ActorContext): EntityPropsProvider[String] = {
    val aggregateMetrics = PersistentActor.createMetrics(metrics, businessLogic.aggregateName)
    val sharedResources = persistence.PersistentEntitySharedResources(kafkaProducerActor, aggregateMetrics, aggregateKafkaStreamsImpl)

    actorId: String => PersistentActor.props(actorId, businessLogic, sharedResources, config)
  }

}
