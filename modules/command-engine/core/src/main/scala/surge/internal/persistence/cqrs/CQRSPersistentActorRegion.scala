// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence.cqrs

import akka.actor.{ ActorContext, ActorSystem }
import org.apache.kafka.common.TopicPartition
import play.api.libs.json.JsValue
import surge.akka.cluster.{ EntityPropsProvider, PerShardLogicProvider }
import surge.core.{ KafkaProducerActor, SurgeCommandBusinessLogic }
import surge.internal.utils.Logging
import surge.kafka.PersistentActorRegionCreator
import surge.kafka.streams.{ AggregateStateStoreKafkaStreams, HealthCheck }
import surge.metrics.Metrics

import scala.concurrent.Future

class CQRSPersistentActorRegionCreator[Agg, Command, Event](
    system: ActorSystem,
    businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event],
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue],
    metrics: Metrics) extends PersistentActorRegionCreator[String] {
  override def regionFromTopicPartition(topicPartition: TopicPartition): PerShardLogicProvider[String] = {
    new CQRSPersistentActorRegion(system, topicPartition, businessLogic, kafkaStreamsCommand, metrics)
  }
}
class CQRSPersistentActorRegion[Agg, Command, Event](
    system: ActorSystem,
    assignedPartition: TopicPartition,
    businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    metrics: Metrics) extends PerShardLogicProvider[String] with Logging {

  val kafkaProducerActor: KafkaProducerActor = KafkaProducerActor(
    actorSystem = system,
    assignedPartition = assignedPartition,
    metrics = metrics,
    businessLogic = businessLogic,
    kStreams = aggregateKafkaStreamsImpl)

  override def actorProvider(context: ActorContext): EntityPropsProvider[String] = {
    val aggregateMetrics = CQRSPersistentActor.createMetrics(metrics, businessLogic.aggregateName)
    val sharedResources = SurgeCQRSPersistentEntitySharedResources(kafkaProducerActor, aggregateMetrics, aggregateKafkaStreamsImpl)

    actorId: String => CQRSPersistentActor.props(actorId, businessLogic, sharedResources)
  }

  override def onShardTerminated(): Unit = {
    log.debug("Shard for partition {} terminated, killing partition kafkaProducerActor", assignedPartition)
    kafkaProducerActor.terminate()
  }

  override def healthCheck(): Future[HealthCheck] = {
    kafkaProducerActor.healthCheck()
  }
}
