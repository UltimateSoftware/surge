// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import org.apache.kafka.streams.scala.kstream.KStream

trait KafkaPartitionMetadataHandler {
  def processPartitionMetadata(stream: KStream[String, KafkaPartitionMetadata]): Unit = {}
}

object NoOpKafkaPartitionMetadataHandler extends KafkaPartitionMetadataHandler
