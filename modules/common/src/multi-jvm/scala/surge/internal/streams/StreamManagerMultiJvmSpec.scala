// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import akka.NotUsed
import akka.kafka.Subscriptions
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.stream.scaladsl.Flow
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import io.opentelemetry.api.trace.Tracer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer, Serializer, StringDeserializer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import surge.internal.akka.kafka.AkkaKafkaConsumer
import surge.internal.tracing.NoopTracerFactory
import surge.kafka.KafkaTopic
import surge.kafka.streams.DefaultSerdes
import surge.streams.replay.{DefaultEventReplaySettings, KafkaForeverReplaySettings, KafkaForeverReplayStrategy, NoOpEventReplayStrategy}
import surge.streams.{DataHandler, EventPlusStreamMeta}

import scala.concurrent.duration._

trait StreamManagerMultiNodeSpec extends MultiNodeSpecCallbacks with AnyWordSpecLike with Matchers with BeforeAndAfterAll {
  self: MultiNodeSpec =>

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  // Might not be needed anymore if we find a nice way to tag all logging from a node
  override implicit def convertToWordSpecStringWrapper(s: String): WordSpecStringWrapper =
    new WordSpecStringWrapper(s"$s (on node '${self.myself.name}', $getClass)")
}

object StreamManagerSpecConfig extends MultiNodeConfig {
  val node0: RoleName = role("node0")
  val node1: RoleName = role("node1")
  val nodesConfig: Config = ConfigFactory.parseString("""
    akka.actor.allow-java-serialization=on
    akka.actor.warn-about-java-serializer-usage=off
    """)
  commonConfig(nodesConfig)
}

class StreamManagerSpecMultiJvmNode0 extends StreamManagerSpecBase
class StreamManagerSpecMultiJvmNode1 extends StreamManagerSpecBase

class StreamManagerSpecBase
    extends MultiNodeSpec(StreamManagerSpecConfig)
    with StreamManagerMultiNodeSpec
    with EmbeddedKafka
    with ScalaFutures
    with OptionValues {
  import StreamManagerSpecConfig._

  val tracer: Tracer = NoopTracerFactory.create()
  override def initialParticipants: Int = roles.size

  "StreamManagerSpec" should {
    "Replay a topic in a cluster environment" in {
      implicit val stringSerializer: Serializer[String] = DefaultSerdes.stringSerde.serializer()
      // FIXME: try to use dynamically allocated Embedded Kafka ports
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 9093)
      implicit val stringDeserializer: Deserializer[String] = DefaultSerdes.stringSerde.deserializer()
      implicit val byteDeserializer: Deserializer[Array[Byte]] = DefaultSerdes.byteArraySerde.deserializer()
      val topicName = "testTopic5"
      val topic = KafkaTopic(topicName)
      val record1 = "record 1"
      val record2 = "record 2"
      def sendToTestProbe(testProbe: TestProbe): DataHandler[String, Array[Byte]] = new DataHandler[String, Array[Byte]] {
        override def dataHandler[Meta]: Flow[EventPlusStreamMeta[String, Array[Byte], Meta], Meta, NotUsed] =
          Flow[EventPlusStreamMeta[String, Array[Byte], Meta]].map { eventPlusOffset =>
            val msg = stringDeserializer.deserialize("", eventPlusOffset.messageBody)
            testProbe.ref ! msg
            eventPlusOffset.streamMeta
          }
      }
      val embeddedBroker = s"${node(node0).address.host.getOrElse("localhost")}:${config.kafkaPort}"
      val consumerSettings = AkkaKafkaConsumer.consumerSettings[String, Array[Byte]](system, "replay-test").withBootstrapServers(embeddedBroker)

      runOn(node0) {
        withRunningKafka {
          createCustomTopic(topic.name, partitions = 2)
          publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
          publishToKafka(new ProducerRecord[String, String](topic.name, 1, record2, record2))
          def postReplayDef(): Unit = {
            enterBarrier("afterReplay")
            ()
          }
          val replaySettings = KafkaForeverReplaySettings(topic.name).copy(brokers = List(embeddedBroker))
          val kafkaForeverReplayStrategy = KafkaForeverReplayStrategy.create(actorSystem = system, settings = replaySettings, postReplay = postReplayDef)

          val probe = TestProbe()
          val subscriptionProvider =
            new KafkaOffsetManagementSubscriptionProvider(topic.name, Subscriptions.topics(topic.name), consumerSettings, sendToTestProbe(probe))
          val consumer = new KafkaStreamManager(
            topicName = topic.name,
            consumerSettings = consumerSettings,
            subscriptionProvider = subscriptionProvider,
            keyDeserializer = new StringDeserializer,
            valueDeserializer = new ByteArrayDeserializer,
            replayStrategy = kafkaForeverReplayStrategy,
            replaySettings = replaySettings,
            tracer = tracer)

          consumer.start()
          probe.expectMsgAnyOf(20.seconds, record1, record2)
          consumer.replay()
          probe.expectMsgAnyOf(20.seconds, record1, record2)
        }
      }

      runOn(node1) {
        val probe = TestProbe()
        val subscriptionProvider =
          new KafkaOffsetManagementSubscriptionProvider(topic.name, Subscriptions.topics(topic.name), consumerSettings, sendToTestProbe(probe))
        val consumer = new KafkaStreamManager(
          topicName = topic.name,
          consumerSettings = consumerSettings,
          subscriptionProvider = subscriptionProvider,
          keyDeserializer = new StringDeserializer,
          valueDeserializer = new ByteArrayDeserializer,
          replayStrategy = new NoOpEventReplayStrategy,
          replaySettings = DefaultEventReplaySettings,
          tracer = tracer)

        consumer.start()
        probe.expectMsgAnyOf(20.seconds, record1, record2)
        enterBarrier("afterReplay")
        probe.expectMsgAnyOf(40.seconds, record1, record2)
      }
    }
  }
}
