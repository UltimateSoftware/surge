// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.kafka

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.ultimatesoftware.kafka.streams.DefaultSerdes
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class StreamManagerSpec extends TestKit(ActorSystem("StreamManagerSpec")) with AnyWordSpecLike with Matchers with EmbeddedKafka with Eventually {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(10, Millis)))

  private implicit val ex: ExecutionContext = ExecutionContext.global
  private implicit val stringSer: Serializer[String] = DefaultSerdes.stringSerde.serializer()
  private val stringDeser = DefaultSerdes.stringSerde.deserializer()

  private def sendToTestProbe(testProbe: TestProbe)(key: String, value: Array[Byte]): Future[Done] = {
    val msg = stringDeser.deserialize("", value)
    testProbe.ref ! msg
    Future.successful(Done)
  }

  private def testStreamManager(topic: KafkaTopic, kafkaBrokers: String, groupId: String,
    businessLogic: (String, Array[Byte]) ⇒ Future[_]): KafkaStreamManager[String, Array[Byte]] = {
    val consumerSettings = KafkaConsumer.defaultConsumerSettings(system, groupId)
      .withBootstrapServers(kafkaBrokers)

    KafkaStreamManager(topic, consumerSettings, businessLogic)
  }

  "StreamManager" should {
    "Subscribe to events from Kafka" in {
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig ⇒
        val topic = KafkaTopic("testTopic")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"
        val probe = TestProbe()

        def createManager: KafkaStreamManager[String, Array[Byte]] =
          testStreamManager(topic, kafkaBrokers = embeddedBroker, groupId = "subscription-test", sendToTestProbe(probe))

        val record1 = "record 1"
        val record2 = "record 2"
        val record3 = "record 3"
        val record4 = "record 4"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
        publishToKafka(new ProducerRecord[String, String](topic.name, 1, record2, record2))
        publishToKafka(new ProducerRecord[String, String](topic.name, 2, record3, record3))
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record4, record4))

        val consumer1 = createManager
        val consumer2 = createManager

        consumer1.start()
        consumer2.start()

        probe.expectMsgAllOf(10.seconds, record1, record2, record3, record4)
        consumer1.stop()
        consumer2.stop()
      }
    }

    "Continue processing elements from Kafka when the business future completes, even if it does not emit an element" in {
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig ⇒
        val topic = KafkaTopic("testTopic")
        createCustomTopic(topic.name, partitions = 1)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"
        val probe = TestProbe()

        // Returning null here when the future completes gets us the same result as converting from a Java Future that completes with null,
        // which is typical in cases where the future is just used to signal completion and doesn't care about the return value
        def handler(key: String, value: Array[Byte]): Future[Any] = sendToTestProbe(probe)(key, value)
          .flatMap(_ ⇒ Future.successful(null)) // scalastyle:ignore null

        val record1 = "record 1"
        val record2 = "record 2"
        val record3 = "record 3"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record2, record2))
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record3, record3))

        val consumer = testStreamManager(topic, kafkaBrokers = embeddedBroker, groupId = "subscription-test", handler)
        consumer.start()
        probe.expectMsgAllOf(10.seconds, record1, record2, record3)
        consumer.stop()
      }
    }

    "Restart the stream if it fails" in {
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig ⇒
        val topic = KafkaTopic("testTopic2")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"

        val probe = TestProbe()
        // TODO The group manager needs withGroupInstanceId enabled to support fast restarts without consumer group rebalance
        //  but the CMP Kafka brokers isn't a high enough version to support that yet.  Once it's updated set the expectedNumExceptions
        //  to 3 to verify we're restarting without rebalancing the consumer group as well.
        val expectedNumExceptions = 1
        var exceptionCount = 0

        def businessLogic(key: String, value: Array[Byte]): Future[Done] = {
          if (exceptionCount < expectedNumExceptions) {
            exceptionCount = exceptionCount + 1
            throw new RuntimeException("This is expected")
          }
          probe.ref ! stringDeser.deserialize(topic.name, value)
          Future.successful(Done)
        }

        def createManager: KafkaStreamManager[String, Array[Byte]] =
          testStreamManager(topic, kafkaBrokers = embeddedBroker, groupId = "restart-test", businessLogic)

        val record1 = "record 1"
        val record2 = "record 2"
        val record3 = "record 3"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
        publishToKafka(new ProducerRecord[String, String](topic.name, 1, record2, record2))
        publishToKafka(new ProducerRecord[String, String](topic.name, 2, record3, record3))

        val consumer1 = createManager

        consumer1.start()

        probe.expectMsgAllOf(20.seconds, record1, record2, record3)
        consumer1.stop()
      }
    }

    "Be able to stop the stream" in {
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig ⇒
        val topic = KafkaTopic("testTopic")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"
        val probe = TestProbe()

        def createManager: KafkaStreamManager[String, Array[Byte]] =
          testStreamManager(topic, kafkaBrokers = embeddedBroker, groupId = "stop-test", sendToTestProbe(probe))

        val record1 = "record 1"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))

        val consumer = createManager

        consumer.start()
        probe.expectMsg(20.seconds, record1)

        consumer.stop()
        val record2 = "record 2"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record2, record2))
        probe.expectNoMessage()

        consumer.start()
        probe.expectMsg(20.seconds, record2)
      }
    }
  }
}
