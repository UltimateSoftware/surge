// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka

import java.time.Instant

import akka.actor.ActorSelection
import org.apache.kafka.common.TopicPartition
import surge.akka.cluster.PerShardLogicProvider

case class PartitionRegion(partitionNumber: Int, regionManager: ActorSelection, assignedSince: Instant, isLocal: Boolean)

trait PersistentActorRegionCreator[IdType] {
  def regionFromTopicPartition(topicPartition: TopicPartition): PerShardLogicProvider[IdType]
}
