// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import akka.actor.ActorSystem
import akka.kafka.Subscriptions
import akka.stream.scaladsl.Flow
import akka.testkit.{ TestKit, TestProbe }
import akka.{ Done, NotUsed }
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar.mock
import surge.internal.akka.streams.FlowConverter
import surge.internal.tracing.NoopTracerFactory
import surge.kafka.KafkaTopic
import surge.kafka.streams.DefaultSerdes
import surge.streams.DataPipeline._
import surge.streams.replay._
import surge.streams.{ DataHandler, EventPlusStreamMeta, KafkaDataSourceConfigHelper }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class StreamManagerSpec
    extends TestKit(ActorSystem("StreamManagerSpec"))
    with AnyWordSpecLike
    with Matchers
    with EmbeddedKafka
    with Eventually
    with BeforeAndAfterAll
    with ScalaFutures {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(60, Seconds)), interval = scaled(Span(50, Millis)))

  private implicit val ex: ExecutionContext = ExecutionContext.global
  private implicit val stringSer: Serializer[String] = DefaultSerdes.stringSerde.serializer()
  private implicit val stringDeserializer: Deserializer[String] = DefaultSerdes.stringSerde.deserializer()
  private implicit val byteArrayDeserializer: Deserializer[Array[Byte]] = DefaultSerdes.byteArraySerde.deserializer()

  private val defaultConfig = ConfigFactory.load()

  private val tracer = NoopTracerFactory.create()

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  private def sendToTestProbe(testProbe: TestProbe)(key: String, value: Array[Byte]): Future[Done] = {
    val msg = stringDeserializer.deserialize("", value)
    testProbe.ref ! msg
    Future.successful(Done)
  }

  private def testStreamManager(
      topic: KafkaTopic,
      kafkaBrokers: String,
      groupId: String,
      businessLogic: (String, Array[Byte]) => Future[_],
      replayStrategy: EventReplayStrategy = new NoOpEventReplayStrategy(),
      replaySettings: EventReplaySettings = DefaultEventReplaySettings): KafkaStreamManager[String, Array[Byte]] = {
    val consumerSettings = KafkaDataSourceConfigHelper.consumerSettingsFromConfig[String, Array[Byte]](system, defaultConfig, kafkaBrokers, groupId)

    val parallelism = 16
    val tupleFlow: (String, Array[Byte], Map[String, Array[Byte]]) => Future[_] = { (k, v, _) => businessLogic(k, v) }
    val partitionBy: (String, Array[Byte], Map[String, Array[Byte]]) => String = { (k, _, _) => k }
    val businessFlow = new DataHandler[String, Array[Byte]] {
      override def dataHandler[Meta]: Flow[EventPlusStreamMeta[String, Array[Byte], Meta], Meta, NotUsed] =
        FlowConverter.flowFor[String, Array[Byte], Meta](
          "test-sink",
          tupleFlow,
          partitionBy,
          mock[DefaultDataSinkExceptionHandler[String, Array[Byte]]],
          parallelism)
    }
    val subscriptionProvider =
      new KafkaOffsetManagementSubscriptionProvider(defaultConfig, topic.name, Subscriptions.topics(topic.name), consumerSettings, businessFlow)(tracer)
    new KafkaStreamManager(
      topic.name,
      consumerSettings,
      subscriptionProvider,
      stringDeserializer,
      byteArrayDeserializer,
      replayStrategy,
      replaySettings,
      defaultConfig)(tracer)
  }

  "StreamManager" should {

    "Subscribe to events from Kafka" in {
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig =>
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
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig =>
        val topic = KafkaTopic("testTopic")
        createCustomTopic(topic.name, partitions = 1)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"
        val probe = TestProbe()

        // Returning null here when the future completes gets us the same result as converting from a Java Future that completes with null,
        // which is typical in cases where the future is just used to signal completion and doesn't care about the return value
        def handler(key: String, value: Array[Byte]): Future[Any] =
          sendToTestProbe(probe)(key, value).flatMap(_ => Future.successful(null)) // scalastyle:ignore null

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
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig =>
        val topic = KafkaTopic("testTopic2")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"

        val expectedNumExceptions = 3
        var exceptionCount = 0

        var receivedRecords: Seq[String] = Seq.empty
        def businessLogic(key: String, value: Array[Byte]): Future[Done] = {
          if (exceptionCount < expectedNumExceptions) {
            exceptionCount = exceptionCount + 1
            throw new RuntimeException("This is expected")
          }
          val record = stringDeserializer.deserialize(topic.name, value)
          receivedRecords = receivedRecords :+ record
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

        eventually {
          receivedRecords should contain(record1)
          receivedRecords should contain(record2)
          receivedRecords should contain(record3)
        }
        consumer1.stop()
      }
    }

    "Be able to stop the stream" in {
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig =>
        val topic = KafkaTopic("testTopic3")
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

    "Be able to replay an empty stream" in {
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig =>
        val topic = KafkaTopic("testTopic4")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"
        val probe = TestProbe()

        val settings = KafkaForeverReplaySettings(defaultConfig, topic.name).copy(brokers = List(embeddedBroker))
        val completeProbe = TestProbe()
        val kafkaForeverReplayStrategy =
          new KafkaForeverReplayStrategyImpl(defaultConfig, system, settings, () => Future.successful(true), () => completeProbe.ref ! ReplayComplete)
        val consumer =
          testStreamManager(topic, kafkaBrokers = embeddedBroker, groupId = "replay-test", sendToTestProbe(probe), kafkaForeverReplayStrategy, settings)

        consumer.start()

        // fix: if we attempt to replay prior to a stream manager being registered for the topic replay will fail.
        //  we need a to ensure the streamManager is started prior to calling replay.
        Thread.sleep(5.seconds.toMillis)

        val replayResult = consumer.replay().futureValue(Timeout(settings.entireReplayTimeout))
        replayResult shouldBe a[ReplaySuccessfullyStarted]

        completeProbe.expectMsg(40.seconds, ReplayComplete)
      }
    }

    "Be able to replay a stream" in {
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig =>
        val topic = KafkaTopic("testTopic4")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"
        val probe = TestProbe()

        val record1 = "record 1"
        val record2 = "record 2"
        val record3 = "record 3"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
        publishToKafka(new ProducerRecord[String, String](topic.name, 1, record2, record2))
        publishToKafka(new ProducerRecord[String, String](topic.name, 2, record3, record3))

        val settings = KafkaForeverReplaySettings(defaultConfig, topic.name).copy(brokers = List(embeddedBroker))
        val completeProbe = TestProbe()
        val kafkaForeverReplayStrategy =
          new KafkaForeverReplayStrategyImpl(defaultConfig, system, settings, () => Future.successful(true), () => completeProbe.ref ! ReplayComplete)
        val consumer =
          testStreamManager(topic, kafkaBrokers = embeddedBroker, groupId = "replay-test", sendToTestProbe(probe), kafkaForeverReplayStrategy, settings)

        consumer.start()
        probe.expectMsgAllOf(20.seconds, record1, record2, record3)
        val replayResult = consumer.replay().futureValue(Timeout(settings.entireReplayTimeout))
        replayResult shouldBe a[ReplaySuccessfullyStarted]
        probe.expectMsgAllOf(40.seconds, record1, record2, record3)

        completeProbe.expectMsg(40.seconds, ReplayComplete)
      }
    }
  }
}
