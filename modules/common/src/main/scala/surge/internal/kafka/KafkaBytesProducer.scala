// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.common.serialization.{ ByteArraySerializer, Serializer, StringSerializer }
import surge.kafka.{ KafkaPartitionerBase, KafkaProducerHelper, KafkaTopic, KafkaTopicTrait, NoPartitioner }

import java.util.Properties
import scala.jdk.CollectionConverters._

object KafkaBytesProducer {
  def create(brokers: java.util.Collection[String], topic: KafkaTopic): KafkaBytesProducer = {
    KafkaBytesProducer(ConfigFactory.load(), brokers.asScala.toSeq, topic)
  }

  def apply(
      config: Config,
      brokers: Seq[String],
      topic: KafkaTopic,
      partitioner: KafkaPartitionerBase[String] = NoPartitioner[String],
      kafkaConfig: Map[String, String] = Map.empty): KafkaBytesProducer = {
    new KafkaBytesProducer(
      brokers,
      topic,
      new StringSerializer(),
      new ByteArraySerializer(),
      partitioner,
      KafkaProducerHelper.producerPropsFromConfig(config, kafkaConfig))
  }

  def apply(brokers: Seq[String], topic: KafkaTopicTrait, partitioner: KafkaPartitionerBase[String], producerProps: Properties): KafkaBytesProducer = {
    new KafkaBytesProducer(brokers, topic, new StringSerializer(), new ByteArraySerializer(), partitioner, producerProps)
  }
}

class KafkaBytesProducer(
    brokers: Seq[String],
    override val topic: KafkaTopicTrait,
    keySerializer: Serializer[String],
    valueSerializer: Serializer[Array[Byte]],
    override val partitioner: KafkaPartitionerBase[String],
    override val producerProps: Properties)
    extends GenericKafkaProducer[String, Array[Byte]](brokers, topic, keySerializer, valueSerializer, partitioner, producerProps)
