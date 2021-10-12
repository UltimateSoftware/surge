// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka

import com.typesafe.config.{ Config, ConfigFactory }

import java.util.Properties
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{ ByteArraySerializer, Serializer, StringSerializer }

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3
import scala.util.{ Failure, Success, Try }

final case class KafkaRecordMetadata[Key](key: Option[Key], wrapped: RecordMetadata)

private[surge] object KafkaProducerHelper {
  def producerPropsFromConfig(config: Config, additionalProps: Map[String, String] = Map.empty): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.ACKS_CONFIG, config.getString("kafka.publisher.acks"))
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getInt("kafka.publisher.batch-size").toString)
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.getInt("kafka.publisher.max-request-size").toString)
    props.put(ProducerConfig.LINGER_MS_CONFIG, config.getInt("kafka.publisher.linger-ms").toString)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getString("kafka.publisher.compression-type"))

    val securityHelper = new KafkaSecurityConfigurationImpl(config)
    securityHelper.configureSecurityProperties(props)

    additionalProps.foreach(propPair => props.put(propPair._1, propPair._2))

    props
  }
}

trait KafkaProducerHelperCommon[K, V] {
  def topic: KafkaTopicTrait
  def partitioner: KafkaPartitionerBase[K]
  def producer: org.apache.kafka.clients.producer.KafkaProducer[K, V]

  lazy val numberPartitions: Int = producer.partitionsFor(topic.name).size

  protected def getPartitionFor(key: K): Option[Int] = {
    partitioner.optionalPartitionBy.flatMap(partitionFun => getPartitionFor(key, numberPartitions, partitionFun))
  }
  protected def getPartitionFor(key: K, numPartitions: Int, keyToPartitionString: K => String): Option[Int] = {
    val partitionByString = keyToPartitionString(key)
    if (partitionByString.isEmpty) {
      None
    } else {
      val partitionNumber = math.abs(MurmurHash3.stringHash(partitionByString) % numPartitions)
      Some(partitionNumber)
    }
  }

  private def recordWithPartition(record: ProducerRecord[K, V]): ProducerRecord[K, V] = {
    // If the record is already partitioned, don't change the partitioning
    val partitionOpt = Option(record.partition()).map(_.intValue()).orElse(getPartitionFor(record.key()))
    partitionOpt
      .map { partitionNum =>
        new ProducerRecord(record.topic, partitionNum, record.timestamp, record.key, record.value, record.headers)
      }
      .getOrElse(record)
  }

  private def producerCallback(record: ProducerRecord[K, V], promise: Promise[KafkaRecordMetadata[K]]): Callback = {
    producerCallback(record, result => promise.complete(result))
  }
  private def producerCallback(record: ProducerRecord[K, V], callback: Try[KafkaRecordMetadata[K]] => Unit): Callback =
    (metadata: RecordMetadata, exception: Exception) => {
      Option(exception) match {
        case Some(e) => callback(Failure(e))
        case _ =>
          val kafkaMeta = KafkaRecordMetadata[K](Option(record.key()), metadata)
          callback(Success(kafkaMeta))
      }
    }

  protected def doPutRecord(record: ProducerRecord[K, V]): Future[KafkaRecordMetadata[K]] = {
    // Since the Kafka interface returns a java Future instead of a scala future we can leverage the
    // producer send with a callback to get a scala future when the write is completed
    val promise = Promise[KafkaRecordMetadata[K]]()
    try {
      val partitionedRecord = recordWithPartition(record)
      producer.send(partitionedRecord, producerCallback(partitionedRecord, promise))
    } catch {
      case NonFatal(e) => promise.failure(e)
    }

    promise.future
  }

  protected def makeRecord(value: V): ProducerRecord[K, V] = {
    new ProducerRecord[K, V](topic.name, value)
  }
  protected def makeRecord(keyValuePair: (K, V)): ProducerRecord[K, V] = {
    new ProducerRecord[K, V](topic.name, keyValuePair._1, keyValuePair._2)
  }
  def makeRecord(key: K, value: V, headers: Headers): ProducerRecord[K, V] = {
    // Using null here since we need to add the headers but we don't want to explicitly assign the partition
    new ProducerRecord[K, V](topic.name, null, key, value, headers) // scalastyle:ignore null
  }
  def beginTransaction(): Unit =
    producer.beginTransaction()

  def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String): Unit =
    producer.sendOffsetsToTransaction(offsets.asJava, consumerGroupId)

  def commitTransaction(): Unit =
    producer.commitTransaction()

  def abortTransaction(): Unit =
    producer.abortTransaction()

  def close(): Unit =
    producer.close()
}

trait KafkaProducerTrait[K, V] extends KafkaProducerHelperCommon[K, V] {
  def partitionFor(key: K): Option[Int] = getPartitionFor(key)

  def putRecord(record: ProducerRecord[K, V]): Future[KafkaRecordMetadata[K]] = doPutRecord(record)
  def putRecords(records: Seq[ProducerRecord[K, V]]): Seq[Future[KafkaRecordMetadata[K]]] = {
    records.map(doPutRecord)
  }

  def putValue(value: V): Future[KafkaRecordMetadata[K]] = {
    doPutRecord(makeRecord(value))
  }
  def putValues(values: Seq[V]): Seq[Future[KafkaRecordMetadata[K]]] = {
    putRecords(values.map(makeRecord))
  }

  def putKeyValue(keyValuePair: (K, V)): Future[KafkaRecordMetadata[K]] = {
    doPutRecord(makeRecord(keyValuePair))
  }
  def putKeyValues(keyValues: Seq[(K, V)]): Seq[Future[KafkaRecordMetadata[K]]] = {
    putRecords(keyValues.map(makeRecord))
  }

  def initTransactions()(implicit ec: ExecutionContext): Future[Unit] = Future {
    producer.initTransactions()
  }
}

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
    KafkaStringProducer(brokers, topic, partitioner, KafkaProducerHelper.producerPropsFromConfig(config, kafkaConfig))
  }
}
case class KafkaStringProducer(
    brokers: Seq[String],
    override val topic: KafkaTopic,
    override val partitioner: KafkaPartitionerBase[String],
    producerProps: Properties)
    extends KafkaProducerTrait[String, String] {

  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  override val producer: org.apache.kafka.clients.producer.KafkaProducer[String, String] =
    new org.apache.kafka.clients.producer.KafkaProducer[String, String](producerProps)
}

object KafkaProducer {
  def bytesProducer(
      config: Config,
      brokers: Seq[String],
      topic: KafkaTopicTrait,
      partitioner: KafkaPartitionerBase[String] = NoPartitioner[String],
      kafkaConfig: Map[String, String] = Map.empty): KafkaProducer[String, Array[Byte]] = {
    KafkaProducer[String, Array[Byte]](
      brokers,
      topic,
      new StringSerializer(),
      new ByteArraySerializer(),
      partitioner,
      KafkaProducerHelper.producerPropsFromConfig(config, kafkaConfig))
  }
}
case class KafkaProducer[K, V](
    brokers: Seq[String],
    override val topic: KafkaTopicTrait,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    override val partitioner: KafkaPartitionerBase[K],
    producerProps: Properties)
    extends KafkaProducerTrait[K, V] {

  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)

  override val producer: org.apache.kafka.clients.producer.KafkaProducer[K, V] =
    new org.apache.kafka.clients.producer.KafkaProducer[K, V](producerProps)
}
