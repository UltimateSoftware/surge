// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka

import java.time.Instant

import akka.actor.{ ActorSelection, Props }
import org.apache.kafka.common.TopicPartition

case class PartitionRegion(partitionNumber: Int, regionManager: ActorSelection, assignedSince: Instant, isLocal: Boolean)

trait TopicPartitionRegionCreator {
  def propsFromTopicPartition(topicPartition: TopicPartition): Props
}
