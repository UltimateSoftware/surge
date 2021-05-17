// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.StreamsMetadata
import surge.kafka.HostPort

import scala.jdk.CollectionConverters._

abstract class KafkaStreamsPartitionTracker(kafkaStreams: KafkaStreams) {
  protected def allMetadata(): Iterable[StreamsMetadata] = kafkaStreams.allMetadata().asScala
  protected def metadataByInstance(): Map[HostPort, List[TopicPartition]] =
    allMetadata().groupBy(meta => HostPort(meta.host(), meta.port())).map { case (key, meta) => key -> meta.toList.flatMap(_.topicPartitions().asScala) }

  def update(): Unit
}

trait KafkaStreamsPartitionTrackerProvider {
  def create(streams: KafkaStreams): KafkaStreamsPartitionTracker
}
