// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor._
import com.typesafe.config.Config
import surge.health.HealthSignalBusTrait
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.core.SurgePartitionRouterImpl
import surge.internal.health.HealthyComponent
import surge.internal.persistence.BusinessLogic
import surge.kafka.{ KafkaProducerTrait, PersistentActorRegionCreator }
import surge.kafka.streams._
import scala.concurrent.ExecutionContext

trait SurgePartitionRouter extends HealthyComponent {
  def actorRegion: ActorRef
}

object SurgePartitionRouter {
  def apply(
      config: Config,
      system: ActorSystem,
      partitionTracker: KafkaConsumerPartitionAssignmentTracker,
      businessLogic: BusinessLogic,
      kafkaStreamsCommand: AggregateStateStoreKafkaStreams,
      regionCreator: PersistentActorRegionCreator[String],
      signalBus: HealthSignalBusTrait,
      isAkkaClusterEnabled: Boolean,
      kafkaProducerOverride: Option[KafkaProducerTrait[String, Array[Byte]]] = None)(implicit ec: ExecutionContext): SurgePartitionRouter = {
    new SurgePartitionRouterImpl(
      config,
      system,
      partitionTracker,
      businessLogic,
      kafkaStreamsCommand,
      regionCreator,
      signalBus,
      isAkkaClusterEnabled,
      kafkaProducerOverride)
  }
}
