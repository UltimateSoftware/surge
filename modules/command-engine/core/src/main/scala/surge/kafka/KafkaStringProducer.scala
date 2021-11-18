// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.kafka

import org.apache.kafka.common.serialization.Serializer

import java.util.Properties

class KafkaStringProducer(brokers: Seq[String],
                          topic: KafkaTopicTrait,
                          keySerializer: Serializer[String],
                          valueSerializer: Serializer[String],
                          partitioner: KafkaPartitionerBase[String],
                          producerProps: Properties) extends surge.internal.kafka.KafkaStringProducer(brokers,
  topic,
  keySerializer,
  valueSerializer,
  partitioner,
  producerProps) {

}
