// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.Serializer
import surge.kafka.{KafkaPartitionerBase, KafkaProducerTrait, KafkaTopicTrait}

import java.util.Properties

class GenericKafkaProducer[K, V](
                                  brokers: Seq[String],
                                  override val topic: KafkaTopicTrait,
                                  keySerializer: Serializer[K],
                                  valueSerializer: Serializer[V],
                                  override val partitioner: KafkaPartitionerBase[K],
                                  override val producerProps: Properties)
  extends KafkaProducerTrait[K, V] {

  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass.getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass.getName)

  override val producer: KafkaProducer[K, V] =
    new KafkaProducer[K, V](producerProps)
}
