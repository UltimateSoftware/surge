// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import akka.actor.ActorSystem
import org.apache.kafka.streams.scala.kstream.KStream
import surge.kafka.streams.KafkaPartitionMetadataHandlerImpl.KafkaPartitionMetadataUpdated
import surge.support.Logging

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
