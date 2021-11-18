// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import surge.kafka.{KafkaPartitionerBase, KafkaProducerHelper, KafkaTopic, KafkaTopicTrait, NoPartitioner}

import java.util.Properties
import scala.jdk.CollectionConverters._

object KafkaStringProducer {
  def create(brokers: java.util.Collection[String], topic: KafkaTopic): KafkaStringProducer = {
    KafkaStringProducer(ConfigFactory.load(), brokers.asScala.toSeq, topic)
  }

  def apply(
             config: Config,
             brokers: Seq[String],
             topic: KafkaTopic,
             partitioner: KafkaPartitionerBase[String] = NoPartitioner[String],
             kafkaConfig: Map[String, String] = Map.empty): KafkaStringProducer = {
    new KafkaStringProducer(
      brokers,
      topic,
      new StringSerializer(),
      new StringSerializer(),
      partitioner,
      KafkaProducerHelper.producerPropsFromConfig(config, kafkaConfig))
  }

  def apply(brokers: Seq[String], topic: KafkaTopicTrait, partitioner: KafkaPartitionerBase[String], producerProps: Properties): KafkaStringProducer = {
    new KafkaStringProducer(brokers, topic, new StringSerializer(), new StringSerializer(), partitioner, producerProps)
  }
}

class KafkaStringProducer(
                           brokers: Seq[String],
                           override val topic: KafkaTopicTrait,
                           keySerializer: Serializer[String],
                           valueSerializer: Serializer[String],
                           override val partitioner: KafkaPartitionerBase[String],
                           override val producerProps: Properties)
  extends GenericKafkaProducer[String, String](brokers, topic, keySerializer, valueSerializer, partitioner, producerProps)

