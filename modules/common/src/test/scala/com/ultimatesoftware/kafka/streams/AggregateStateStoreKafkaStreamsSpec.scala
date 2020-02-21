// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import com.ultimatesoftware.scala.core.kafka.{ JsonSerdes, KafkaTopic }
import com.ultimatesoftware.scala.core.utils.JsonUtils
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest.{ BeforeAndAfter, Matchers, WordSpec }
import play.api.libs.json.{ Format, JsValue, Json }

class MockPartitionTrackerProvider extends KafkaStreamsPartitionTrackerProvider {
  override def create(streams: KafkaStreams): KafkaStreamsPartitionTracker = new MockPartitionTracker(streams)
}

class MockPartitionTracker(streams: KafkaStreams) extends KafkaStreamsPartitionTracker(streams: KafkaStreams) {
  override def update(): Unit = {
  }
}

class MockPartitionMetaHandler extends KafkaPartitionMetadataHandler {
}

object MockState {
  implicit val format: Format[MockState] = Json.format
}

case class MockState(string: String, int: Int)
class AggregateStateStoreKafkaStreamsSpec extends WordSpec with Matchers with BeforeAndAfter with KafkaStreamsTestHelpers {
  private val stateTopic: KafkaTopic = KafkaTopic("testStateTopic")

  // Silly mock validator that expects the `string` field of a MockState to be "stateN" where N is the value of the MockState int
  // to trigger valid/invalid validation results
  private def mockValidator(key: String, newValue: Array[Byte], oldValue: Option[Array[Byte]]): Boolean = {
    val newValueObj = JsonUtils.parseMaybeCompressedBytes[MockState](newValue).get
    newValueObj.string == "state" + newValueObj.int
  }

  private val aggStoreKafkaStreams = new AggregateStateStoreKafkaStreams[MockState](
    aggregateName = "test",
    stateTopic = stateTopic,
    partitionTrackerProvider = new MockPartitionTrackerProvider,
    kafkaStateMetadataHandler = new MockPartitionMetaHandler,
    aggregateValidator = mockValidator,
    applicationHostPort = Some("localhost:1234"))

  "AggregateStateStoreKafkaStreams" should {
    "Store key value pairs from Kafka in a KTable" in withTopologyTestDriver(aggStoreKafkaStreams.createTopology()) { testDriver ⇒
      val state1 = MockState("state1", 1)
      val state2 = MockState("state2", 2)
      val state3 = MockState("state3", 3)
      val invalidValidationState = MockState("invalidValidation", 1)
      val factory = new ConsumerRecordFactory[String, MockState](stateTopic.name, new StringSerializer(),
        JsonSerdes.serdeFor[MockState].serializer())

      testDriver.pipeInput(factory.create(stateTopic.name, state1.string, state1))
      testDriver.pipeInput(factory.create(stateTopic.name, state2.string, state2))
      testDriver.pipeInput(factory.create(stateTopic.name, state3.string, state3))
      testDriver.pipeInput(factory.create(stateTopic.name, invalidValidationState.string, invalidValidationState))

      val store = testDriver.getKeyValueStore[String, JsValue](aggStoreKafkaStreams.aggregateStateStore)
      store.get(state1.string) shouldEqual Json.toJson(state1).toString().getBytes
      store.get(state2.string) shouldEqual Json.toJson(state2).toString().getBytes
      store.get(state3.string) shouldEqual Json.toJson(state3).toString().getBytes
      store.get(invalidValidationState.string) shouldEqual Json.toJson(invalidValidationState).toString().getBytes

      val updated1 = state1.copy(int = 3)
      testDriver.pipeInput(factory.create(stateTopic.name, updated1.string, updated1))
      store.get(state1.string) shouldEqual Json.toJson(updated1).toString().getBytes
    }
  }
}
