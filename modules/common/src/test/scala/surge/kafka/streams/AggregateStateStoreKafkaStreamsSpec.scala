// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import java.util.UUID
import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import io.github.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier
import org.apache.kafka.streams.{ KafkaStreams, TopologyTestDriver }
import org.scalatest.concurrent.{ PatienceConfiguration, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{ Assertion, BeforeAndAfterAll }
import org.scalatestplus.mockito.MockitoSugar
import play.api.libs.json.{ Format, JsValue, Json }
import surge.core.Ack
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

class MockPartitionTrackerProvider extends KafkaStreamsPartitionTrackerProvider {
  override def create(streams: KafkaStreams): KafkaStreamsPartitionTracker = new MockPartitionTracker(streams)
}

class MockPartitionTracker(streams: KafkaStreams) extends KafkaStreamsPartitionTracker(streams: KafkaStreams) {
  override def update(): Unit = {}
}

object MockState {
  implicit val format: Format[MockState] = Json.format
}

case class MockState(string: String, int: Int) {
  def toJsString: String = Json.toJson(this).toString()
}
class AggregateStateStoreKafkaStreamsSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with KafkaStreamsTestHelpers
    with ScalaFutures
    with EmbeddedKafka
    with MockitoSugar
    with PatienceConfiguration {
  import surge.internal.health.context.TestContext._
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(10, Millis)) // scalastyle:ignore magic.number

  private val system = ActorSystem("test-actor-system")

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  private val defaultConfig = ConfigFactory.load()

  "AggregateStateStoreKafkaStreams" should {
    def assertStoreKeyValue(testDriver: TopologyTestDriver, stateTopic: KafkaTopic, aggStoreKafkaStreams: AggregateStateStoreKafkaStreams): Assertion = {
      val state1 = MockState("state1", 1)
      val state2 = MockState("state2", 2)
      val state3 = MockState("state3", 3)
      val invalidValidationState = MockState("invalidValidation", 1)
      val inputTopic = testDriver.createInputTopic(stateTopic.name, new StringSerializer, new StringSerializer)

      inputTopic.pipeInput(state1.string, state1.toJsString)
      inputTopic.pipeInput(state2.string, state2.toJsString)
      inputTopic.pipeInput(state3.string, state3.toJsString)
      inputTopic.pipeInput(invalidValidationState.string, invalidValidationState.toJsString)

      val store = testDriver.getKeyValueStore[String, JsValue](aggStoreKafkaStreams.settings.storeName)
      store.get(state1.string) shouldEqual Json.toJson(state1).toString().getBytes
      store.get(state2.string) shouldEqual Json.toJson(state2).toString().getBytes
      store.get(state3.string) shouldEqual Json.toJson(state3).toString().getBytes
      store.get(invalidValidationState.string) shouldEqual Json.toJson(invalidValidationState).toString().getBytes

      val updated1 = state1.copy(int = 3)
      inputTopic.pipeInput(updated1.string, updated1.toJsString)
      store.get(state1.string) shouldEqual Json.toJson(updated1).toString().getBytes
    }

    "Store key value pairs from Kafka in a KTable" in {
      withRunningKafkaOnFoundPort(config) { implicit actualConfig =>
        val topicName = "testStateTopic"
        createCustomTopic(topicName)
        val stateTopic: KafkaTopic = KafkaTopic(topicName)

        val testAggregateName = "test"
        val appId = s"aggregate-streams-spec-${UUID.randomUUID()}"
        val aggStoreKafkaStreams = new AggregateStateStoreKafkaStreams(
          aggregateName = testAggregateName,
          stateTopic = stateTopic,
          partitionTrackerProvider = new MockPartitionTrackerProvider,
          applicationHostPort = Some("localhost:1234"),
          applicationId = appId,
          clientId = "",
          testHealthSignalStreamProvider(Seq.empty).bus(),
          system,
          Metrics.globalMetricRegistry,
          defaultConfig) {
          override lazy val settings: SurgeAggregateStoreSettings =
            SurgeAggregateStoreSettings(defaultConfig, appId, testAggregateName, "", Some("localhost:1234")).copy(brokers =
              Seq(s"localhost:${actualConfig.kafkaPort}"))
        }

        aggStoreKafkaStreams.controllable.start()

        val topology = aggStoreKafkaStreams.getTopology.futureValue

        withTopologyTestDriver(topology) { testDriver =>
          assertStoreKeyValue(testDriver, stateTopic, aggStoreKafkaStreams)
        }
        aggStoreKafkaStreams.controllable.stop().futureValue shouldBe an[Ack]
      }
    }
    "Restart the stream on any errors" in {
      val exceptionThrowingConfig = ConfigFactory
        .parseString("""
          exception-throwing {
            plugin-class = "surge.kafka.streams.SingleExceptionThrowingPersistenceProvider"
          }
          surge.kafka-streams.state-store-plugin = exception-throwing
          """.stripMargin)
        .withFallback(ConfigFactory.load())
      withRunningKafkaOnFoundPort(config) { implicit actualConfig =>
        val topicName = "testStateTopic"
        createCustomTopic(topicName)
        val stateTopic: KafkaTopic = KafkaTopic(topicName)

        val testAggregateName = "test"
        val appId = s"aggregate-streams-spec-${UUID.randomUUID()}"
        val aggStoreKafkaStreams = new AggregateStateStoreKafkaStreams(
          aggregateName = testAggregateName,
          stateTopic = stateTopic,
          partitionTrackerProvider = new MockPartitionTrackerProvider,
          applicationHostPort = Some("localhost:1234"),
          applicationId = appId,
          clientId = "",
          testHealthSignalStreamProvider(Seq.empty).bus(),
          system,
          Metrics.globalMetricRegistry,
          exceptionThrowingConfig) {
          override lazy val settings: SurgeAggregateStoreSettings =
            SurgeAggregateStoreSettings(defaultConfig, appId, testAggregateName, "", Some("localhost:1234")).copy(brokers =
              Seq(s"localhost:${actualConfig.kafkaPort}"))
        }

        aggStoreKafkaStreams.controllable.start()

        val topology = aggStoreKafkaStreams.getTopology.futureValue

        withTopologyTestDriver(topology) { testDriver =>
          an[Exception] should be thrownBy assertStoreKeyValue(testDriver, stateTopic, aggStoreKafkaStreams) // Initial failure will propagate to test driver
          // if we make it to use the stream it means it restarted correctly after the crash
          assertStoreKeyValue(testDriver, stateTopic, aggStoreKafkaStreams)
        }
        aggStoreKafkaStreams.controllable.stop().futureValue shouldBe an[Ack]
      }
    }
  }
}

class SingleExceptionThrowingPersistenceProvider extends SurgeKafkaStreamsPersistencePlugin {
  override def createSupplier(storeName: String): KeyValueBytesStoreSupplier = new SingleExceptionThrowingKeyValueStoreSupplier(storeName)
  override def enableLogging: Boolean = true
}
