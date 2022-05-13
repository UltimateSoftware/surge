// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import org.apache.kafka.common.{ PartitionInfo, TopicPartition }
import org.slf4j.LoggerFactory

import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ ExecutorService, Executors }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

final case class UltiKafkaConsumerConfig(consumerGroup: String, offsetReset: String = "earliest", pollDuration: FiniteDuration = 3.seconds) {
  def kafkaClientProps: Map[String, String] = {
    Map(ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset)
  }
}

private[kafka] object KafkaPollThread {
  private val threadNumberCount: AtomicLong = new AtomicLong(0)
  def createNewPollThread: ExecutorService = Executors.newSingleThreadExecutor((r: Runnable) => {
    val thread = new Thread(r, s"kafka-poll-${threadNumberCount.getAndIncrement()}")
    thread.setDaemon(true)
    thread
  })
}

private[surge] object KafkaConsumerHelper {
  def consumerPropsFromConfig(config: Config, consumerConfig: UltiKafkaConsumerConfig, additionalProps: Map[String, String] = Map.empty): Properties = {
    val props = new Properties
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString)

    consumerConfig.kafkaClientProps.foreach(propPair => props.put(propPair._1, propPair._2))

    val securityHelper = new KafkaSecurityConfigurationImpl(config)
    securityHelper.configureSecurityProperties(props)

    additionalProps.foreach(propPair => props.put(propPair._1, propPair._2))

    props
  }
}

abstract class KafkaConsumerTrait[K, V] {
  def consumerConfig: UltiKafkaConsumerConfig
  def consumer: KafkaConsumer[K, V]

  private val log = LoggerFactory.getLogger(getClass)

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

object KafkaStringConsumer {
  def apply(config: Config, brokers: Seq[String], consumerConfig: UltiKafkaConsumerConfig, kafkaConfig: Map[String, String]): KafkaStringConsumer = {
    KafkaStringConsumer(brokers, consumerConfig, KafkaConsumerHelper.consumerPropsFromConfig(config, consumerConfig, kafkaConfig))
  }
}
final case class KafkaStringConsumer(brokers: Seq[String], override val consumerConfig: UltiKafkaConsumerConfig, consumerProps: Properties)
    extends KafkaConsumerTrait[String, String] {
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  override val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProps)
}

object KafkaBytesConsumer {
  def apply(config: Config, brokers: Seq[String], consumerConfig: UltiKafkaConsumerConfig, kafkaConfig: Map[String, String]): KafkaBytesConsumer = {
    KafkaBytesConsumer(brokers, consumerConfig, KafkaConsumerHelper.consumerPropsFromConfig(config, consumerConfig, kafkaConfig))
  }
}
final case class KafkaBytesConsumer(brokers: Seq[String], override val consumerConfig: UltiKafkaConsumerConfig, consumerProps: Properties)
    extends KafkaConsumerTrait[String, Array[Byte]] {
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)

  override val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](consumerProps)
}
