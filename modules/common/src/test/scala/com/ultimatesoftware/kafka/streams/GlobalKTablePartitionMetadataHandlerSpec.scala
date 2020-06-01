// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.util.Properties

import akka.actor.{ ActorSystem, Props }
import com.ultimatesoftware.scala.core.kafka.{ JsonSerdes, KafkaTopic }
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.{ StreamsConfig, TopologyTestDriver }
import org.scalatest.{ Assertion, BeforeAndAfterAll }
import org.scalatest.concurrent.{ PatienceConfiguration, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext
import akka.pattern.{ BackoffOpts, BackoffSupervisor }
import com.ultimatesoftware.kafka.streams.GlobalKTableMetadataHandlerImpl.GlobalKTableMetadataHandlerImplSettings
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._

class GlobalKTablePartitionMetadataHandlerSpec
  extends AnyWordSpec
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll
  with KafkaStreamsTestHelpers
  with MockitoSugar
  with PatienceConfiguration
  with EmbeddedKafka {

  override implicit val patienceConfig = PatienceConfig(
    timeout = Span(10, Seconds), interval = Span(10, Millis)) // scalastyle:ignore magic.number

  private val metaTopic = KafkaTopic("testMetaTopic")
  private val system = ActorSystem("test-actor-system")
  private val environment = "development"
  private val consumerGroupName = s"global-ktable-$environment-${metaTopic.name}"

  import system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  "GlobalKTableMetadataHandler" should {

    def followUpdatesToInternalMetadataTopic(testDriver: TopologyTestDriver, globalMetaHandler: GlobalKTableMetadataHandler): Assertion = {
      val meta1 = KafkaPartitionMetadata("test", 0, 10L, "foo")
      val meta2 = KafkaPartitionMetadata("test", 1, 1L, "bar")
      val meta3 = KafkaPartitionMetadata("otherTopic", 0, 10L, "baz")

      val inputTopic = testDriver.createInputTopic(metaTopic.name, new StringSerializer, JsonSerdes.serdeFor[KafkaPartitionMetadata].serializer())

      inputTopic.pipeInput(meta1.topicPartition, meta1)
      inputTopic.pipeInput(meta2.topicPartition, meta2)
      inputTopic.pipeInput(meta3.topicPartition, meta3)

      val store = testDriver.getKeyValueStore[String, JsValue](globalMetaHandler.settings.storeName)
      store.get(meta1.topicPartition) shouldEqual meta1
      store.get(meta2.topicPartition) shouldEqual meta2
      store.get(meta3.topicPartition) shouldEqual meta3

      val updated1 = meta1.copy(offset = 12L, key = "someOtherAggregate")
      inputTopic.pipeInput(updated1.topicPartition, updated1)
      store.get(meta1.topicPartition) shouldEqual updated1
    }

    val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234")

    "Follow updates to the internal metadata topic" in {
      withRunningKafkaOnFoundPort(config) { implicit actualConfig ⇒
        val globalMetaHandler = new GlobalKTableMetadataHandler(metaTopic, consumerGroupName, system) {
          override lazy val settings = GlobalKTableMetadataHandlerImplSettings(consumerGroupName, metaTopic.name).copy(
            brokers = List(s"localhost:${actualConfig.kafkaPort}"))
        }
        val topology = globalMetaHandler.getTopology().futureValue

        withTopologyTestDriver(topology, props) { testDriver ⇒
          followUpdatesToInternalMetadataTopic(testDriver, globalMetaHandler)
        }
      }
    }
    "Successfully restart after an error" in {
      withRunningKafkaOnFoundPort(config) { implicit actualConfig ⇒
        val topicMock = mock[KafkaTopic]
        // First access to topic.name happens before actor initialization, second access is inside the actor
        // Actor first attempt to start the stream will throw an exception, then it will succeed
        when(topicMock.name).thenReturn(metaTopic.name).thenThrow(new RuntimeException("This is expected")).thenReturn(metaTopic.name)
        val globalMetaHandler = new GlobalKTableMetadataHandler(metaTopic, consumerGroupName, system) {
          override lazy val settings = GlobalKTableMetadataHandlerImplSettings(consumerGroupName, metaTopic.name).copy(
            brokers = List(s"localhost:${actualConfig.kafkaPort}"))
          override def createBackoffSupervisorFor(childProps: Props): Props = {
            BackoffSupervisor.props(
              BackoffOpts.onStop(
                childProps,
                childName = settings.storeName,
                minBackoff = 100.millis,
                maxBackoff = 200.millis,
                randomFactor = 0))
          }
        }

        val topology = globalMetaHandler.getTopology().futureValue
        // tell the actor to crash
        withTopologyTestDriver(topology, props) { testDriver ⇒
          followUpdatesToInternalMetadataTopic(testDriver, globalMetaHandler)
        }
      }
    }
  }
}
