// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.util.Properties

import com.ultimatesoftware.scala.core.kafka.{ JsonSerdes, KafkaTopic }
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.{ StreamsConfig, TopologyTestDriver }
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest.{ Matchers, WordSpec }
import play.api.libs.json.{ JsValue, Json }

class GlobalKTablePartitionMetadataHandlerSpec extends WordSpec with Matchers {
  private val metaTopic = KafkaTopic("testMetaTopic")

  private val globalMetaHandler = new GlobalKTableMetadataHandler(metaTopic)
  private val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234")

  "GlobalKTableMetadataHandler" should {
    "Follow updates to the internal metadata topic" in {
      val testDriver = new TopologyTestDriver(globalMetaHandler.createTopology(), props)
      val meta1 = KafkaPartitionMetadata("test", 0, 10L, "foo")
      val meta2 = KafkaPartitionMetadata("test", 1, 1L, "bar")
      val meta3 = KafkaPartitionMetadata("otherTopic", 0, 10L, "baz")

      val factory = new ConsumerRecordFactory[String, KafkaPartitionMetadata](metaTopic.name, new StringSerializer(),
        JsonSerdes.serdeFor[KafkaPartitionMetadata].serializer())

      testDriver.pipeInput(factory.create(metaTopic.name, meta1.topicPartition, meta1))
      testDriver.pipeInput(factory.create(metaTopic.name, meta2.topicPartition, meta2))
      testDriver.pipeInput(factory.create(metaTopic.name, meta3.topicPartition, meta3))

      val store = testDriver.getKeyValueStore[String, JsValue](globalMetaHandler.globalStateMetaStoreName)
      store.get(meta1.topicPartition) shouldEqual meta1
      store.get(meta2.topicPartition) shouldEqual meta2
      store.get(meta3.topicPartition) shouldEqual meta3

      val updated1 = meta1.copy(offset = 12L, key = "someOtherAggregate")
      testDriver.pipeInput(factory.create(metaTopic.name, updated1.topicPartition, updated1))
      store.get(meta1.topicPartition) shouldEqual updated1

      testDriver.close()
    }
  }
}
