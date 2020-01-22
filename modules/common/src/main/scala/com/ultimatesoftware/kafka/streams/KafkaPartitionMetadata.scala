// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import org.apache.kafka.streams.processor.ProcessorContext
import play.api.libs.json.{ Format, Json }

object KafkaPartitionMetadata {
  implicit val format: Format[KafkaPartitionMetadata] = Json.format

  def fromContext(context: ProcessorContext, key: String): KafkaPartitionMetadata = {
    KafkaPartitionMetadata(topic = context.topic(), partition = context.partition(), offset = context.offset(), key = key)
  }
}

case class KafkaPartitionMetadata(topic: String, partition: Int, offset: Long, key: String)
