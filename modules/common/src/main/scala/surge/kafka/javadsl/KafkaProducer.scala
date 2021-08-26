// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.javadsl

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }
import surge.kafka._

import java.util.concurrent.CompletionStage
import java.util.{ Optional, Properties }
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

abstract class AbstractKafkaProducer[K, V] extends KafkaProducerHelperCommon[K, V] {
  private implicit val executionContext: ExecutionContext = ExecutionContext.global

  def partitionFor(key: K): Optional[Int] = getPartitionFor(key).asJava

  def putRecord(record: ProducerRecord[K, V]): CompletionStage[RecordMetadata] = doPutRecord(record).map(_.wrapped).toJava

  def putValue(value: V): CompletionStage[RecordMetadata] = {
    doPutRecord(makeRecord(value)).map(_.wrapped).toJava
  }

  def putKeyValue(key: K, value: V): CompletionStage[RecordMetadata] = {
    doPutRecord(makeRecord(key -> value)).map(_.wrapped).toJava
  }

  def initTransactions(): Unit = {
    producer.initTransactions()
  }
}

object KafkaBytesProducer {
  private val config = ConfigFactory.load()
  def create(brokers: java.util.Collection[String], topic: KafkaTopic): KafkaBytesProducer = {
    create(brokers, topic, PartitionStringUpToColon.instance, config, Map.empty[String, String].asJava)
  }
  def create(brokers: java.util.Collection[String], topic: KafkaTopic, kafkaConfig: java.util.Map[String, String]): KafkaBytesProducer = {
    create(brokers, topic, PartitionStringUpToColon.instance, config, kafkaConfig)
  }

  def create(
      brokers: java.util.Collection[String],
      topic: KafkaTopic,
      partitioner: KafkaPartitionerBase[String],
      config: Config,
      kafkaConfig: java.util.Map[String, String]): KafkaBytesProducer = {
    new KafkaBytesProducer(brokers, topic, partitioner, KafkaProducerHelper.producerPropsFromConfig(config, kafkaConfig.asScala.toMap))
  }
}
class KafkaBytesProducer(
    brokers: java.util.Collection[String],
    override val topic: KafkaTopic,
    override val partitioner: KafkaPartitionerBase[String],
    producerProps: Properties)
    extends AbstractKafkaProducer[String, Array[Byte]] {

  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.asScala.mkString(","))
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
  override val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer[String, Array[Byte]](producerProps)
}
