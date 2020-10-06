// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.AggregateStateStoreKafkaStreamsImpl.AggregateStateStoreKafkaStreamsImplSettings
import com.ultimatesoftware.scala.core.kafka.{ JsonSerdes, KafkaTopic }
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{ KafkaStreams, TopologyTestDriver }
import org.scalatest.{ Assertion, BeforeAndAfterAll }
import org.scalatest.concurrent.{ PatienceConfiguration, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar
import play.api.libs.json.{ Format, JsValue, Json }
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any

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
class AggregateStateStoreKafkaStreamsSpec
  extends AnyWordSpec
  with Matchers
  with BeforeAndAfterAll
  with KafkaStreamsTestHelpers
  with ScalaFutures
  with EmbeddedKafka
  with MockitoSugar
  with PatienceConfiguration {

  override implicit val patienceConfig = PatienceConfig(
    timeout = Span(30, Seconds), interval = Span(10, Millis)) // scalastyle:ignore magic.number

  private val system = ActorSystem("test-actor-system")

  override def afterAll() {
    system.terminate()
  }

  // Silly mock validator that expects the `string` field of a MockState to be "stateN" where N is the value of the MockState int
  // to trigger valid/invalid validation results
  private def mockValidator(key: String, newValue: Array[Byte], oldValue: Option[Array[Byte]]): Boolean = {
    val newValueObj = Json.parse(newValue).as[MockState]
    newValueObj.string == "state" + newValueObj.int
  }
  val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  "AggregateStateStoreKafkaStreams" should {
    def assertStoreKeyValue(
      testDriver: TopologyTestDriver,
      stateTopic: KafkaTopic,
      aggStoreKafkaStreams: AggregateStateStoreKafkaStreams[MockState]): Assertion = {
      val state1 = MockState("state1", 1)
      val state2 = MockState("state2", 2)
      val state3 = MockState("state3", 3)
      val invalidValidationState = MockState("invalidValidation", 1)
      val inputTopic = testDriver.createInputTopic(stateTopic.name, new StringSerializer, JsonSerdes.serdeFor[MockState].serializer())

      inputTopic.pipeInput(state1.string, state1)
      inputTopic.pipeInput(state2.string, state2)
      inputTopic.pipeInput(state3.string, state3)
      inputTopic.pipeInput(invalidValidationState.string, invalidValidationState)

      val store = testDriver.getKeyValueStore[String, JsValue](aggStoreKafkaStreams.settings.storeName)
      store.get(state1.string) shouldEqual Json.toJson(state1).toString().getBytes
      store.get(state2.string) shouldEqual Json.toJson(state2).toString().getBytes
      store.get(state3.string) shouldEqual Json.toJson(state3).toString().getBytes
      store.get(invalidValidationState.string) shouldEqual Json.toJson(invalidValidationState).toString().getBytes

      val updated1 = state1.copy(int = 3)
      inputTopic.pipeInput(updated1.string, updated1)
      store.get(state1.string) shouldEqual Json.toJson(updated1).toString().getBytes
    }

    "Store key value pairs from Kafka in a KTable" in {
      withRunningKafkaOnFoundPort(config) { implicit actualConfig ⇒
        val topicName = "testStateTopic"
        createCustomTopic(topicName)
        val stateTopic: KafkaTopic = KafkaTopic(topicName)

        val testAggregateName = "test"
        val testConsumerGroupName = "test-aggregate-consumer-group-name"
        val aggStoreKafkaStreams = new AggregateStateStoreKafkaStreams[MockState](
          aggregateName = testAggregateName,
          stateTopic = stateTopic,
          partitionTrackerProvider = new MockPartitionTrackerProvider,
          kafkaStateMetadataHandler = new MockPartitionMetaHandler,
          aggregateValidator = mockValidator,
          applicationHostPort = Some("localhost:1234"),
          consumerGroupName = testConsumerGroupName,
          system) {
          override lazy val settings = AggregateStateStoreKafkaStreamsImplSettings(testAggregateName, testConsumerGroupName).copy(
            brokers = Seq(s"localhost:${actualConfig.kafkaPort}"))
        }

        val topology = aggStoreKafkaStreams.getTopology().futureValue

        withTopologyTestDriver(topology) { testDriver ⇒
          assertStoreKeyValue(testDriver, stateTopic, aggStoreKafkaStreams)
        }
      }
    }
    "Restart the stream on any errors" in {
      withRunningKafkaOnFoundPort(config) { implicit actualConfig ⇒
        val topicName = "testStateTopic"
        createCustomTopic(topicName)
        val stateTopic: KafkaTopic = KafkaTopic(topicName)

        val mockedStateMetaHandler = spy(new MockPartitionMetaHandler())
        // Kafka streams will fail to initialize in the first attempt
        doThrow(new RuntimeException("This is Expected")).doNothing().when(mockedStateMetaHandler).processPartitionMetadata(any[KStream[String, KafkaPartitionMetadata]])

        val testAggregateName = "test"
        val testConsumerGroupName = "test-aggregate-consumer-group-name"
        val aggStoreKafkaStreams = new AggregateStateStoreKafkaStreams[MockState](
          aggregateName = testAggregateName,
          stateTopic = stateTopic,
          partitionTrackerProvider = new MockPartitionTrackerProvider,
          kafkaStateMetadataHandler = mockedStateMetaHandler,
          aggregateValidator = mockValidator,
          applicationHostPort = Some("localhost:1234"),
          consumerGroupName = testConsumerGroupName,
          system) {
          override lazy val settings = AggregateStateStoreKafkaStreamsImplSettings(testConsumerGroupName, testAggregateName).copy(
            brokers = Seq(s"localhost:${actualConfig.kafkaPort}"))
        }

        val topology = aggStoreKafkaStreams.getTopology().futureValue

        withTopologyTestDriver(topology) { testDriver ⇒
          // if we make it to use the stream it means it restarted correctly after the crash
          assertStoreKeyValue(testDriver, stateTopic, aggStoreKafkaStreams)
        }
      }
    }
  }
}
