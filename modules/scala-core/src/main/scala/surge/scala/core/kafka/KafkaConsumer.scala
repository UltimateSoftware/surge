// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import org.apache.kafka.common.{ PartitionInfo, TopicPartition }

import scala.collection.JavaConverters._
import scala.concurrent.duration._

final case class UltiKafkaConsumerConfig(consumerGroup: String, offsetReset: String = "earliest", pollDuration: FiniteDuration = 3.seconds)

trait KafkaConsumerTrait[K, V] extends KafkaSecurityConfiguration {
  def brokers: Seq[String]
  def props: Properties
  def consumerConfig: UltiKafkaConsumerConfig

  def consumer: KafkaConsumer[K, V]

  protected val defaultProps: Properties = {
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
    p.put(ConsumerConfig.GROUP_ID_CONFIG, consumerConfig.consumerGroup)
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerConfig.offsetReset)
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)

    configureSecurityProperties(p)

    p
  }

  private var running = true

  def subscribe(topic: KafkaTopic, businessLogic: ConsumerRecord[K, V] => Unit): Unit = {
    consumer.subscribe(java.util.Arrays.asList(topic.name))
    try {
      while (running) {
        val records = consumer.poll(java.time.Duration.ofMillis(consumerConfig.pollDuration.toMillis))
        for {
          record <- records.iterator().asScala
        } {
          businessLogic(record)
        }
      }
    } finally {
      consumer.close()
    }
  }

  def stop(): Unit = {
    running = false
  }

  def partitionInfo(topic: KafkaTopic): Set[PartitionInfo] = {
    consumer.partitionsFor(topic.name).asScala.toSet
  }
  def partitions(topic: KafkaTopic): Set[TopicPartition] = {
    partitionInfo(topic).map(info => new TopicPartition(info.topic, info.partition))
  }

  def startOffsets(topic: KafkaTopic): Map[TopicPartition, Long] = {
    startOffsets(partitions(topic))
  }
  def startOffsets(partitions: Set[TopicPartition]): Map[TopicPartition, Long] = {
    consumer.beginningOffsets(partitions.asJava).asScala.mapValues(_.toLong).toMap
  }

  def endOffsets(topic: KafkaTopic): Map[TopicPartition, Long] = {
    endOffsets(partitions(topic))
  }
  def endOffsets(partitions: Set[TopicPartition]): Map[TopicPartition, Long] = {
    consumer.endOffsets(partitions.asJava).asScala.mapValues(_.toLong).toMap
  }
}

final case class KafkaStringConsumer(
    override val brokers: Seq[String],
    override val consumerConfig: UltiKafkaConsumerConfig,
    kafkaConfig: Map[String, String]) extends KafkaConsumerTrait[String, String] {
  val props: Properties = {
    val p = defaultProps
    kafkaConfig.foreach(propPair => p.put(propPair._1, propPair._2))
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    p
  }
  override val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
}

final case class KafkaBytesConsumer(
    override val brokers: Seq[String],
    override val consumerConfig: UltiKafkaConsumerConfig,
    kafkaConfig: Map[String, String]) extends KafkaConsumerTrait[String, Array[Byte]] {
  val props: Properties = {
    val p = defaultProps
    kafkaConfig.foreach(propPair => p.put(propPair._1, propPair._2))
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    p
  }

  override val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](props)
}
