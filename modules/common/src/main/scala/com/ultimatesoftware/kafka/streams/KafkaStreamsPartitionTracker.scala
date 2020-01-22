// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import com.ultimatesoftware.scala.core.kafka.HostPort
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.StreamsMetadata

import scala.collection.JavaConverters._

abstract class KafkaStreamsPartitionTracker(kafkaStreams: KafkaStreams) {
  protected def allMetadata(): Iterable[StreamsMetadata] = kafkaStreams.allMetadata().asScala
  protected def metadataByInstance(): Map[HostPort, List[TopicPartition]] = allMetadata()
    .groupBy(meta ⇒ HostPort(meta.host(), meta.port()))
    .mapValues(meta ⇒ meta.toList.flatMap(_.topicPartitions().asScala))

  def update(): Unit
}

trait KafkaStreamsPartitionTrackerProvider {
  def create(streams: KafkaStreams): KafkaStreamsPartitionTracker
}
