// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import org.apache.kafka.streams.scala.kstream.KStream

trait KafkaPartitionMetadataHandler {
  def processPartitionMetadata(stream: KStream[String, KafkaPartitionMetadata]): Unit = {}
}

object NoOpKafkaPartitionMetadataHandler extends KafkaPartitionMetadataHandler
