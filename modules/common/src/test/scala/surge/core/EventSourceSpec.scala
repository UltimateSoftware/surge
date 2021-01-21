// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import java.time.Instant

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.testkit.{ TestKit, TestProbe }
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory
import surge.akka.streams.kafka.KafkaConsumer
import surge.kafka.streams.DefaultSerdes
import surge.metrics.{ MetricsProvider, NoOpMetricsProvider }
import surge.scala.core.kafka.KafkaTopic

import scala.concurrent.Future
import scala.concurrent.duration._

class EventSourceSpec extends TestKit(ActorSystem("EventSourceSpec")) with AnyWordSpecLike with Matchers with EmbeddedKafka with Eventually {
  private val log = LoggerFactory.getLogger(getClass)

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(10, Millis)))

  private implicit val stringSer: Serializer[String] = DefaultSerdes.stringSerde.serializer()

  private def testEventSource(topic: KafkaTopic, kafkaBrokers: String, groupId: String): EventSource[String] = {
    new EventSource[String] {
      override def baseEventName: String = "TestAggregateEvent"
      override def metricsProvider: MetricsProvider = NoOpMetricsProvider
      override def kafkaTopic: KafkaTopic = topic
      override def formatting: SurgeEventReadFormatting[String] = bytes => new String(bytes)
      override def actorSystem: ActorSystem = system
    }
  }

  private def testConsumerSettings(kafkaBrokers: String, groupId: String): ConsumerSettings[String, Array[Byte]] = {
    KafkaConsumer.defaultConsumerSettings(system, groupId)
      .withBootstrapServers(kafkaBrokers)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }

  private def testEventSink(probe: TestProbe): EventSink[String] = new EventSink[String] {
    override def handleEvent(key: String, event: String, headers: Map[String, Array[Byte]]): Future[Any] = {
      probe.ref ! event
      Future.successful(Done)
    }

    override def partitionBy(key: String, event: String, headers: Map[String, Array[Byte]]): String = {
      event
    }
  }

  "EventSource" should {
    "Subscribe to events from Kafka" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val topic = KafkaTopic("testTopic")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"

        val groupId = "subscription-test"
        def createConsumer: EventSource[String] =
          testEventSource(topic, kafkaBrokers = embeddedBroker, groupId = groupId)

        val record1 = "record 1"
        val record2 = "record 2"
        val record3 = "record 3"
        val record4 = "record 4"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
        publishToKafka(new ProducerRecord[String, String](topic.name, 1, record2, record2))
        publishToKafka(new ProducerRecord[String, String](topic.name, 2, record3, record3))
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record4, record4))

        val consumer1 = createConsumer
        val consumer2 = createConsumer
        val consumerSettings = testConsumerSettings(embeddedBroker, groupId)
        val probe = TestProbe()
        val testSink = testEventSink(probe)

        consumer1.to(consumerSettings)(testSink, autoStart = true)
        consumer2.to(consumerSettings)(testSink, autoStart = true)

        probe.expectMsgAllOf(10.seconds, record1, record2, record3, record4)
      }
    }

    "Not automatically subscribe to events if autoStart is false" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val topic = KafkaTopic("testTopic")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"

        val groupId = "auto-start-false-test"

        def createConsumer: EventSource[String] =
          testEventSource(topic, kafkaBrokers = embeddedBroker, groupId = groupId)

        val record1 = "record 1"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))

        val consumer1 = createConsumer
        val consumerSettings = testConsumerSettings(embeddedBroker, groupId)
        val probe = TestProbe()
        val testSink = testEventSink(probe)

        val pipeline = consumer1.to(consumerSettings)(testSink, autoStart = false)
        probe.expectNoMessage(5.seconds)
        pipeline.start()
        probe.expectMsgAllOf(10.seconds, record1)
      }
    }

    "Read tombstone records" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val topic = KafkaTopic("testTopic")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"

        val groupId = "tombstone-record-test"

        def createConsumer: EventSource[String] =
          testEventSource(topic, kafkaBrokers = embeddedBroker, groupId = groupId)

        val record1 = "record 1"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, null))

        val consumer1 = createConsumer
        val consumerSettings = testConsumerSettings(embeddedBroker, groupId)
        val probe = TestProbe()
        val testSink = new EventSink[String] {
          override def handleEvent(key: String, event: String, headers: Map[String, Array[Byte]]): Future[Any] = {
            probe.ref ! event
            Future.successful(Done)
          }

          override def nullEventFactory(key: String, headers: Map[String, Array[Byte]]): Option[String] = {
            Some(s"DELETE $key")
          }

          override def partitionBy(key: String, event: String, headers: Map[String, Array[Byte]]): String = {
            event
          }
        }

        consumer1.to(consumerSettings)(testSink, autoStart = true)

        probe.expectMsgAllOf(10.seconds, record1, s"DELETE $record1")
      }
    }

    "Restart if an exception is thrown in the stream" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val topic = KafkaTopic("testTopic2")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"

        val groupId = "restart-test"
        def createConsumer: EventSource[String] =
          testEventSource(topic, kafkaBrokers = embeddedBroker, groupId = groupId)

        val record1 = "record 1"
        val record2 = "record 2"
        val record3 = "record 3"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
        publishToKafka(new ProducerRecord[String, String](topic.name, 1, record2, record2))
        publishToKafka(new ProducerRecord[String, String](topic.name, 2, record3, record3))

        val consumer1 = createConsumer

        val probe = TestProbe()
        def createTestSink: EventSink[String] = new EventSink[String] {
          private val expectedNumExceptions = 1
          private var exceptionCount = 0
          override def handleEvent(key: String, event: String, headers: Map[String, Array[Byte]]): Future[Any] = {
            if (exceptionCount < expectedNumExceptions) {
              exceptionCount = exceptionCount + 1
              Future.failed(new RuntimeException("This is expected"))
            } else {
              probe.ref ! event
              Future.successful(Done)
            }
          }
          override def partitionBy(key: String, event: String, headers: Map[String, Array[Byte]]): String = event
        }

        val consumerSettings = testConsumerSettings(embeddedBroker, groupId)
        val testSink1 = createTestSink
        consumer1.to(consumerSettings)(testSink1, autoStart = true)

        probe.expectMsgAllOf(10.seconds, record1, record2, record3)
      }
    }

    "Quick load test" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val topic = KafkaTopic("loadTestTopic")
        createCustomTopic(topic.name, partitions = 3)

        val groupId = "quick-load-test"
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"
        def createConsumer: EventSource[String] =
          testEventSource(topic, kafkaBrokers = embeddedBroker, groupId = groupId)

        (1 to 1000).foreach { num =>
          publishToKafka(new ProducerRecord[String, String](topic.name, num % 3, s"record $num", s"record $num"))
        }
        val consumer1 = createConsumer
        val consumerSettings = testConsumerSettings(embeddedBroker, groupId)
        var count = 0
        val countingEventSink = new EventSink[String] {
          override def handleEvent(key: String, event: String, headers: Map[String, Array[Byte]]): Future[Any] = {
            count += 1
            Future.successful(Done)
          }
          override def partitionBy(key: String, event: String, headers: Map[String, Array[Byte]]): String = event
        }

        consumer1.to(consumerSettings)(countingEventSink, autoStart = true)

        val startTime = Instant.now
        eventually {
          count shouldEqual 1000
        }
        val endTime = Instant.now
        val timeElapsed = endTime.toEpochMilli - startTime.toEpochMilli
        log.info(s"Time taken is: $timeElapsed ms")
        succeed
      }
    }
  }

}
