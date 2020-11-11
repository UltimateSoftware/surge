// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka

import java.time.Instant

import akka.actor.{ ActorSelection, Props }
import org.apache.kafka.common.TopicPartition

case class PartitionRegion(partitionNumber: Int, regionManager: ActorSelection, assignedSince: Instant, isLocal: Boolean)

trait TopicPartitionRegionCreator {
  def propsFromTopicPartition(topicPartition: TopicPartition): Props
}
