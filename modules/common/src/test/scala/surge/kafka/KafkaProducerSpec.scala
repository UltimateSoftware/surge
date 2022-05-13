// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.{ RecordHeader, RecordHeaders }
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import java.util.Properties
import java.util.concurrent.CompletableFuture

class KafkaProducerSpec extends AnyWordSpec with Matchers {
  private val defaultConfig = ConfigFactory.load()

  class MockProducer(val producer: KafkaProducer[String, String]) extends KafkaProducerTrait[String, String] {
    override def topic: KafkaTopic = KafkaTopic("")
    override def partitioner: KafkaPartitionerBase[String] = NoPartitioner[String]

    override def producerProps(): Properties = new Properties
  }
  private def createRecordMeta(topic: String, partition: Int, offset: Int): RecordMetadata = {
    new RecordMetadata(new TopicPartition(topic, partition), 0, offset, Instant.now.toEpochMilli, 0L, 0, 0)
  }
  "KafkaProducer" should {
    "Configure correctly" in {
      val acksConfigOverride = Map(ProducerConfig.ACKS_CONFIG -> "1")
      val producer = surge.kafka.KafkaProducer.bytesProducer(defaultConfig, Seq("localhost:9092"), KafkaTopic("test"), kafkaConfig = acksConfigOverride)

      val props = producer.producerProps()
      props.getProperty(ProducerConfig.ACKS_CONFIG) shouldEqual "1"
    }

    "Maintain the headers for a message when explicitly partitioning a message" in {
      val mockProducer = mock(classOf[KafkaProducer[String, String]])
      val mockRecordMetadata = createRecordMeta("topic", 1, 0)
      when(mockProducer.send(any[ProducerRecord[String, String]], any(classOf[Callback]))).thenReturn(CompletableFuture.completedFuture(mockRecordMetadata))

      val testHeaders = new RecordHeaders.add(new RecordHeader("test", "header".getBytes())).add(new RecordHeader("second", "value".getBytes()))

      val newRecord = new ProducerRecord("topic", 1, Instant.now.toEpochMilli, "key", "value", testHeaders)

      val testProducer = new MockProducer(mockProducer)
      testProducer.putRecord(newRecord)

      verify(mockProducer).send(ArgumentMatchers.eq(newRecord), any(classOf[Callback]))
    }
  }
}
