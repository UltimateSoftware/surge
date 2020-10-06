// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.KafkaPartitionMetadataHandlerImpl.KafkaPartitionMetadataUpdated
import org.apache.kafka.streams.scala.kstream.KStream
import com.ultimatesoftware.support.Logging

class KafkaPartitionMetadataHandlerImpl(system: ActorSystem) extends KafkaPartitionMetadataHandler with Logging {
  override def processPartitionMetadata(stream: KStream[String, KafkaPartitionMetadata]): Unit = {
    stream.mapValues { value ⇒
      log.trace("Updating StateMeta for {} to {}", Seq(value.topicPartition, value): _*)
      // KafkaProducerActor should subscribe to these events
      system.eventStream.publish(KafkaPartitionMetadataUpdated(value))
    }
  }
}

object KafkaPartitionMetadataHandlerImpl {
  case class KafkaPartitionMetadataUpdated(value: KafkaPartitionMetadata)
}
