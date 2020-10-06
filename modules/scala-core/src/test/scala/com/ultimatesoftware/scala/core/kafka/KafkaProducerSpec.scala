// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.kafka

import java.time.Instant
import java.util.Properties
import java.util.concurrent.CompletableFuture

import org.apache.kafka.clients.producer.{ Callback, KafkaProducer, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.{ RecordHeader, RecordHeaders }
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KafkaProducerSpec extends AnyWordSpec with Matchers {
  class MockProducer(val producer: KafkaProducer[String, String]) extends KafkaProducerTrait[String, String] {
    override def brokers: Seq[String] = Seq.empty
    override def topic: KafkaTopic = KafkaTopic("")
    override def props: Properties = new Properties()
    override def partitioner: KafkaPartitionerBase[String] = NoPartitioner[String]
  }
  private def createRecordMeta(topic: String, partition: Int, offset: Int): RecordMetadata = {
    new RecordMetadata(
      new TopicPartition(topic, partition), 0, offset, Instant.now.toEpochMilli,
      0L, 0, 0)
  }
  "KafkaProducer" should {
    "Maintain the headers for a message when explicitly partitioning a message" in {
      val mockProducer = mock(classOf[KafkaProducer[String, String]])
      val mockRecordMetadata = createRecordMeta("topic", 1, 0)
      when(mockProducer.send(any[ProducerRecord[String, String]], any(classOf[Callback]))).thenReturn(CompletableFuture.completedFuture(mockRecordMetadata))

      val testHeaders = new RecordHeaders()
        .add(new RecordHeader("test", "header".getBytes()))
        .add(new RecordHeader("second", "value".getBytes()))

      val newRecord = new ProducerRecord("topic", 1, Instant.now.toEpochMilli, "key", "value", testHeaders)

      val testProducer = new MockProducer(mockProducer)
      testProducer.putRecord(newRecord)

      verify(mockProducer).send(ArgumentMatchers.eq(newRecord), any(classOf[Callback]))
    }
  }
}
