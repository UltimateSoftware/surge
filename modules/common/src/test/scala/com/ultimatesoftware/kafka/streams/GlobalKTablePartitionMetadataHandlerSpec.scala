// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.util.Properties

import com.ultimatesoftware.scala.core.kafka.{ JsonSerdes, KafkaTopic }
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.{ StreamsConfig, TopologyTestDriver }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.JsValue

class GlobalKTablePartitionMetadataHandlerSpec extends AnyWordSpec with Matchers {
  private val metaTopic = KafkaTopic("testMetaTopic")
  private val environment = "development"
  private val consumerGroupName = s"global-ktable-$environment-${metaTopic.name}"
  private val globalMetaHandler = new GlobalKTableMetadataHandler(metaTopic, consumerGroupName)
  private val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234")

  "GlobalKTableMetadataHandler" should {
    "Follow updates to the internal metadata topic" in {
      val testDriver = new TopologyTestDriver(globalMetaHandler.createTopology(), props)
      val meta1 = KafkaPartitionMetadata("test", 0, 10L, "foo")
      val meta2 = KafkaPartitionMetadata("test", 1, 1L, "bar")
      val meta3 = KafkaPartitionMetadata("otherTopic", 0, 10L, "baz")

      val inputTopic = testDriver.createInputTopic(metaTopic.name, new StringSerializer, JsonSerdes.serdeFor[KafkaPartitionMetadata].serializer())

      inputTopic.pipeInput(meta1.topicPartition, meta1)
      inputTopic.pipeInput(meta2.topicPartition, meta2)
      inputTopic.pipeInput(meta3.topicPartition, meta3)

      val store = testDriver.getKeyValueStore[String, JsValue](globalMetaHandler.globalStateMetaStoreName)
      store.get(meta1.topicPartition) shouldEqual meta1
      store.get(meta2.topicPartition) shouldEqual meta2
      store.get(meta3.topicPartition) shouldEqual meta3

      val updated1 = meta1.copy(offset = 12L, key = "someOtherAggregate")
      inputTopic.pipeInput(updated1.topicPartition, updated1)
      store.get(meta1.topicPartition) shouldEqual updated1

      testDriver.close()
    }
  }
}
