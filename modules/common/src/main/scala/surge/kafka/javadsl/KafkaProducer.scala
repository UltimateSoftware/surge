// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.javadsl

import java.util.concurrent.CompletionStage
import java.util.{ Optional, Properties }

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }
import surge.kafka.{ KafkaPartitionerBase, KafkaProducerHelperCommon, KafkaSecurityConfiguration, KafkaTopic, PartitionStringUpToColon }

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

abstract class AbstractKafkaProducer[K, V] extends KafkaSecurityConfiguration with KafkaProducerHelperCommon[K, V] {
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
  def create(brokers: java.util.Collection[String], topic: KafkaTopic): KafkaBytesProducer = {
    new KafkaBytesProducer(brokers, topic, PartitionStringUpToColon, Map.empty[String, String].asJava)
  }
  def create(brokers: java.util.Collection[String], topic: KafkaTopic, kafkaConfig: java.util.Map[String, String]): KafkaBytesProducer = {
    new KafkaBytesProducer(brokers, topic, PartitionStringUpToColon, kafkaConfig)
  }
}
class KafkaBytesProducer(
    brokers: java.util.Collection[String],
    override val topic: KafkaTopic,
    override val partitioner: KafkaPartitionerBase[String],
    kafkaConfig: java.util.Map[String, String])
    extends AbstractKafkaProducer[String, Array[Byte]] {
  val props: Properties = {
    val p = new Properties()
    kafkaConfig.asScala.foreach(propPair => p.put(propPair._1, propPair._2))
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.asScala.mkString(","))
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    configureSecurityProperties(p)
    p
  }
  override val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer[String, Array[Byte]](props)
}
