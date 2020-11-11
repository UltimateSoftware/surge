// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3
import scala.util.{ Failure, Success, Try }

final case class KafkaRecordMetadata[Key](key: Option[Key], wrapped: RecordMetadata)

trait KafkaProducerHelperCommon[K, V] {
  def topic: KafkaTopic
  def partitioner: KafkaPartitionerBase[K]
  def producer: KafkaProducer[K, V]

  lazy val numberPartitions: Int = producer.partitionsFor(topic.name).size

  protected def getPartitionFor(key: K): Option[Int] = {
    partitioner.optionalPartitionBy.flatMap(partitionFun ⇒ getPartitionFor(key, numberPartitions, partitionFun))
  }
  protected def getPartitionFor(key: K, numPartitions: Int, keyToPartitionString: K ⇒ String): Option[Int] = {
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
    val partitionOpt = Option(record.partition()).map(_.intValue()) orElse getPartitionFor(record.key())
    partitionOpt.map { partitionNum ⇒
      new ProducerRecord(record.topic, partitionNum, record.timestamp, record.key, record.value, record.headers)
    }.getOrElse(record)
  }

  private def producerCallback(record: ProducerRecord[K, V], promise: Promise[KafkaRecordMetadata[K]]): Callback = {
    producerCallback(record, result ⇒ promise.complete(result))
  }
  private def producerCallback(record: ProducerRecord[K, V], callback: Try[KafkaRecordMetadata[K]] ⇒ Unit): Callback =
    (metadata: RecordMetadata, exception: Exception) ⇒ {
      Option(exception) match {
        case Some(e) ⇒ callback(Failure(e))
        case _ ⇒
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
      case NonFatal(e) ⇒ promise.failure(e)
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

trait KafkaProducerTrait[K, V] extends KafkaSecurityConfiguration with KafkaProducerHelperCommon[K, V] {
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
    KafkaStringProducer(brokers.asScala.toSeq, topic)
  }
}
case class KafkaStringProducer(
    brokers: Seq[String],
    override val topic: KafkaTopic,
    override val partitioner: KafkaPartitionerBase[String] = NoPartitioner[String],
    kafkaConfig: Map[String, String] = Map.empty) extends KafkaProducerTrait[String, String] {
  val props: Properties = {
    val p = new Properties()
    kafkaConfig.foreach(propPair ⇒ p.put(propPair._1, propPair._2))
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    configureSecurityProperties(p)
    p
  }
  override val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
}

case class KafkaBytesProducer(
    brokers: Seq[String],
    override val topic: KafkaTopic,
    override val partitioner: KafkaPartitionerBase[String] = NoPartitioner[String],
    kafkaConfig: Map[String, String] = Map.empty) extends KafkaProducerTrait[String, Array[Byte]] {

  val props: Properties = {
    val p = new Properties()
    kafkaConfig.foreach(propPair ⇒ p.put(propPair._1, propPair._2))
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    configureSecurityProperties(p)
    p
  }
  override val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer[String, Array[Byte]](props)
}
