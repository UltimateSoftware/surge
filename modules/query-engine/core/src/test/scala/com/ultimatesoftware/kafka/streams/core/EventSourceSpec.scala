// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.testkit.{ TestKit, TestProbe }
import com.ultimatesoftware.akka.streams.kafka.KafkaConsumer
import com.ultimatesoftware.kafka.streams.DefaultSerdes
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

class EventSourceSpec extends TestKit(ActorSystem("EventSourceSpec")) with AnyWordSpecLike with Matchers with EmbeddedKafka with Eventually {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(10, Millis)))

  private implicit val stringSer: Serializer[String] = DefaultSerdes.stringSerde.serializer()
  private def testEventSource(topic: KafkaTopic, kafkaBrokers: String, groupId: String): EventSource[String, String] = {
    new EventSource[String, String] {
      override def kafkaTopic: KafkaTopic = topic
      override def parallelism: Int = 1
      override def consumerGroup: String = groupId
      override lazy val consumerSettings: ConsumerSettings[String, Array[Byte]] = KafkaConsumer.consumerSettings(system, groupId)
        .withBootstrapServers(kafkaBrokers)
      override def formatting: SurgeEventReadFormatting[String, String] = (bytes: Array[Byte]) ⇒ new String(bytes) -> Some("")
    }
  }

  private def testEventSink(probe: TestProbe): EventSink[String, String] = (event: String, _: String) ⇒ {
    probe.ref ! event
    Future.successful(Done)
  }

  "EventSource" should {
    "Subscribe to events from Kafka" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig ⇒
        val topic = KafkaTopic("testTopic")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"

        def createConsumer: EventSource[String, String] =
          testEventSource(topic, kafkaBrokers = embeddedBroker, groupId = "subscription-test")

        val record1 = "record 1"
        val record2 = "record 2"
        val record3 = "record 3"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
        publishToKafka(new ProducerRecord[String, String](topic.name, 1, record2, record2))
        publishToKafka(new ProducerRecord[String, String](topic.name, 2, record3, record3))

        val consumer1 = createConsumer
        val consumer2 = createConsumer
        val probe = TestProbe()
        val testSink = testEventSink(probe)

        consumer1.to(testSink)
        consumer2.to(testSink)

        probe.expectMsgAllOf(10.seconds, record1, record2, record3)
      }
    }

    "Restart if an exception is thrown in the stream" ignore {
      // FIXME this test is flaky when not using the new subscriber.  I think it should reliably fail when not
      //  using the new subscriber, but it strangely seems to pass sometimes.
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig ⇒
        val topic = KafkaTopic("testTopic2")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"

        def createConsumer: EventSource[String, String] =
          testEventSource(topic, kafkaBrokers = embeddedBroker, groupId = "restart-test")

        val record1 = "record 1"
        val record2 = "record 2"
        val record3 = "record 3"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
        publishToKafka(new ProducerRecord[String, String](topic.name, 1, record2, record2))
        publishToKafka(new ProducerRecord[String, String](topic.name, 2, record3, record3))

        val consumer1 = createConsumer
        val consumer2 = createConsumer

        val probe = TestProbe()
        val testSink: EventSink[String, String] = new EventSink[String, String] {
          private val log = LoggerFactory.getLogger(getClass)
          private val expectedNumExceptions = 1
          private var exceptionCount = 0
          override def handleEvent(event: String, eventProps: String): Future[Any] = {
            if (exceptionCount < expectedNumExceptions) {
              exceptionCount = exceptionCount + 1
              log.info("Throwing exception!")
              throw new RuntimeException("This is expected")
            }
            log.info("Not throwing exception")
            probe.ref ! event
            Future.successful(Done)
          }
        }

        consumer1.to(testSink)
        consumer2.to(testSink)

        probe.expectMsgAllOf(60.seconds, record1, record2, record3)
      }
    }
  }

}

