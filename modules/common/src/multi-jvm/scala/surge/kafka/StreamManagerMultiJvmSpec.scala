// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka

import akka.NotUsed
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks }
import akka.stream.scaladsl.Flow
import akka.testkit.TestProbe
import com.typesafe.config.{ Config, ConfigFactory }
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, OptionValues }
import surge.akka.streams.kafka.{ KafkaConsumer, KafkaStreamManager, KafkaStreamMeta }
import surge.core._
import surge.kafka.streams.DefaultSerdes
import surge.scala.core.kafka.KafkaTopic
import surge.streams.EventPlusStreamMeta
import surge.streams.replay.{ DefaultEventReplaySettings, KafkaForeverReplaySettings, KafkaForeverReplayStrategy, NoOpEventReplayStrategy }

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

class StreamManagerSpecBase extends MultiNodeSpec(StreamManagerSpecConfig) with StreamManagerMultiNodeSpec
  with EmbeddedKafka with ScalaFutures with OptionValues {
  import StreamManagerSpecConfig._

  override def initialParticipants: Int = roles.size

  "StreamManagerSpec" should {
    "Replay a topic in a cluster environment" in {
      implicit val stringSerializer: Serializer[String] = DefaultSerdes.stringSerde.serializer()
      // FIXME: try to use dynamically allocated Embedded Kafka ports
      implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 9093)
      val stringDeserializer = DefaultSerdes.stringSerde.deserializer()
      val topicName = "testTopic5"
      val topic = KafkaTopic(topicName)
      val record1 = "record 1"
      val record2 = "record 2"
      def sendToTestProbe(testProbe: TestProbe): Flow[EventPlusStreamMeta[String, Array[Byte], KafkaStreamMeta], KafkaStreamMeta, NotUsed] = {
        Flow[EventPlusStreamMeta[String, Array[Byte], KafkaStreamMeta]].map { eventPlusOffset =>
          val msg = stringDeserializer.deserialize("", eventPlusOffset.messageBody)
          testProbe.ref ! msg
          eventPlusOffset.streamMeta
        }
      }
      val embeddedBroker = s"${node(node0).address.host.getOrElse("localhost")}:${config.kafkaPort}"
      val consumerSettings = KafkaConsumer.defaultConsumerSettings(system, "replay-test").withBootstrapServers(embeddedBroker)

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
          val kafkaForeverReplayStrategy = KafkaForeverReplayStrategy.create(
            actorSystem = system,
            settings = replaySettings,
            postReplay = postReplayDef
          )

          val probe = TestProbe()
          import probe.system.dispatcher
          val consumer = KafkaStreamManager(topic, consumerSettings, kafkaForeverReplayStrategy, replaySettings, sendToTestProbe(probe))
          consumer.start()
          probe.expectMsgAnyOf(20.seconds, record1, record2)
          consumer.replay()
          probe.expectMsgAnyOf(20.seconds, record1, record2)
        }
      }

      runOn(node1) {
         val probe = TestProbe()
        import probe.system.dispatcher
         val consumer = KafkaStreamManager(topic, consumerSettings, NoOpEventReplayStrategy, DefaultEventReplaySettings, sendToTestProbe(probe))
         consumer.start()
         probe.expectMsgAnyOf(20.seconds, record1, record2)
         enterBarrier("afterReplay")
         probe.expectMsgAnyOf(40.seconds, record1, record2)
      }
    }
  }
}
