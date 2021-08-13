// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor._
import com.typesafe.config.Config
import surge.health.HealthSignalBusTrait
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.core.SurgePartitionRouterImpl
import surge.internal.persistence.BusinessLogic
import surge.kafka.PersistentActorRegionCreator
import surge.kafka.streams._

trait SurgePartitionRouter extends HealthyComponent with Controllable {
  def actorRegion: ActorRef
}

object SurgePartitionRouter {
  def apply(
      config: Config,
      system: ActorSystem,
      partitionTracker: KafkaConsumerPartitionAssignmentTracker,
      businessLogic: BusinessLogic,
      regionCreator: PersistentActorRegionCreator[String],
      signalBus: HealthSignalBusTrait): SurgePartitionRouter = {
    new SurgePartitionRouterImpl(config, system, partitionTracker, businessLogic, regionCreator, signalBus)
  }
}
