// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka

import akka.Done
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.akka.streams.kafka.{KafkaConsumer, KafkaStreamManager}
import com.ultimatesoftware.kafka.streams.DefaultSerdes
import com.ultimatesoftware.kafka.streams.core.NoOpEventReplayStrategy
import com.ultimatesoftware.kafka.streams.core.{ DefaultEventReplaySettings, KafkaForeverReplayStrategy, KafkaForeverReplaySettings }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.concurrent.Future

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
  val nodesConfig = ConfigFactory.parseString("""
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
      def sendToTestProbe(testProbe: TestProbe)(key: String, value: Array[Byte]): Future[Done] = {
        val msg = stringDeserializer.deserialize("", value)
        testProbe.ref ! msg
        Future.successful(Done)
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
          val consumer = KafkaStreamManager(topic, consumerSettings, kafkaForeverReplayStrategy, replaySettings, sendToTestProbe(probe), 1)
          consumer.start()
          probe.expectMsgAnyOf(20.seconds, record1, record2)
          consumer.replay()
          probe.expectMsgAnyOf(20.seconds, record1, record2)
        }
      }

      runOn(node1) {
         val probe = TestProbe()
        import probe.system.dispatcher
         val consumer = KafkaStreamManager(topic, consumerSettings, NoOpEventReplayStrategy, DefaultEventReplaySettings, sendToTestProbe(probe), 1)
         consumer.start()
         probe.expectMsgAnyOf(20.seconds, record1, record2)
         enterBarrier("afterReplay")
         probe.expectMsgAnyOf(40.seconds, record1, record2)
      }
    }
  }
}
