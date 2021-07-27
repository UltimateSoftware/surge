// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka

import com.typesafe.config.Config

import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ ExecutorService, Executors }
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import org.apache.kafka.common.{ PartitionInfo, TopicPartition }
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

final case class UltiKafkaConsumerConfig(consumerGroup: String, offsetReset: String = "earliest", pollDuration: FiniteDuration = 3.seconds)

private[kafka] object KafkaPollThread {
  private val threadNumberCount: AtomicLong = new AtomicLong(0)
  def createNewPollThread: ExecutorService = Executors.newSingleThreadExecutor((r: Runnable) => {
    val thread = new Thread(r, s"kafka-poll-${threadNumberCount.getAndIncrement()}")
    thread.setDaemon(true)
    thread
  })
}

abstract class KafkaConsumerTrait[K, V] extends KafkaSecurityConfiguration {
  def brokers: Seq[String]
  def props: Properties
  def consumerConfig: UltiKafkaConsumerConfig

  def consumer: KafkaConsumer[K, V]

  private val log = LoggerFactory.getLogger(getClass)

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
    val pollThread = KafkaPollThread.createNewPollThread
    pollThread.execute(() => pollKafka(businessLogic))
  }

  private def pollKafka(businessLogic: ConsumerRecord[K, V] => Unit): Unit = {
    try {
      while (running) {
        try {
          val records = consumer.poll(java.time.Duration.ofMillis(consumerConfig.pollDuration.toMillis))
          for {
            record <- records.iterator().asScala
          } {
            businessLogic(record)
          }
        } catch {
          case t: Throwable => log.error("Poll loop failed in KafkaConsumer. Retrying.", t)
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
    consumer.beginningOffsets(partitions.asJava).asScala.map(tup => tup._1 -> tup._2.toLong).toMap
  }

  def endOffsets(topic: KafkaTopic): Map[TopicPartition, Long] = {
    endOffsets(partitions(topic))
  }
  def endOffsets(partitions: Set[TopicPartition]): Map[TopicPartition, Long] = {
    consumer.endOffsets(partitions.asJava).asScala.map(tup => tup._1 -> tup._2.toLong).toMap
  }
}

final case class KafkaStringConsumer(
    override val config: Config,
    override val brokers: Seq[String],
    override val consumerConfig: UltiKafkaConsumerConfig,
    kafkaConfig: Map[String, String])
    extends KafkaConsumerTrait[String, String] {
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
    override val config: Config,
    override val brokers: Seq[String],
    override val consumerConfig: UltiKafkaConsumerConfig,
    kafkaConfig: Map[String, String])
    extends KafkaConsumerTrait[String, Array[Byte]] {
  val props: Properties = {
    val p = defaultProps
    kafkaConfig.foreach(propPair => p.put(propPair._1, propPair._2))
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    p
  }

  override val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](props)
}
