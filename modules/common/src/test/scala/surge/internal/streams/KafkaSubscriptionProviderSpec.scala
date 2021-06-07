// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import akka.actor.ActorSystem
import akka.kafka.Subscriptions
import akka.stream.scaladsl.{ Flow, Sink }
import akka.testkit.{ TestKit, TestProbe }
import akka.{ Done, NotUsed }
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import surge.internal.akka.kafka.AkkaKafkaConsumer
import surge.internal.akka.streams.FlowConverter
import surge.kafka.KafkaTopic
import surge.kafka.streams.DefaultSerdes
import surge.streams.{ DataHandler, EventPlusStreamMeta, OffsetManager }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class KafkaSubscriptionProviderSpec extends TestKit(ActorSystem("StreamManagerSpec")) with AnyWordSpecLike with Matchers with Eventually with EmbeddedKafka {
  private implicit val executionContext: ExecutionContext = ExecutionContext.global
  private implicit val stringSer: Serializer[String] = DefaultSerdes.stringSerde.serializer()
  private implicit val stringDeserializer: Deserializer[String] = DefaultSerdes.stringSerde.deserializer()
  private implicit val byteArrayDeserializer: Deserializer[Array[Byte]] = DefaultSerdes.byteArraySerde.deserializer()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds), interval = Span(100, Millis))

  private class InMemoryOffsetManager() extends OffsetManager {
    private val offsetMappings: scala.collection.mutable.Map[TopicPartition, Long] = scala.collection.mutable.Map.empty

    override def getOffsets(topics: Set[TopicPartition]): Future[Map[TopicPartition, Long]] = Future.successful(offsetMappings.toMap)
    override def commit(topicPartition: TopicPartition, offset: Long): Future[Done] = {
      offsetMappings.put(topicPartition, offset)
      Future.successful(Done)
    }
    def offsetFor(topicPartition: TopicPartition): Option[Long] = offsetMappings.get(topicPartition)
  }

  private def testManualSubscriptionProvider(
      topic: KafkaTopic,
      kafkaBrokers: String,
      groupId: String,
      businessLogic: (String, Array[Byte]) => Future[_],
      offsetManager: OffsetManager): ManualOffsetManagementSubscriptionProvider[String, Array[Byte]] = {
    val consumerSettings = AkkaKafkaConsumer.consumerSettings[String, Array[Byte]](system, groupId).withBootstrapServers(kafkaBrokers)
    val tupleFlow: (String, Array[Byte], Map[String, Array[Byte]]) => Future[_] = { (k, v, _) => businessLogic(k, v) }
    val partitionBy: (String, Array[Byte], Map[String, Array[Byte]]) => String = { (k, _, _) => k }
    val businessFlow = new DataHandler[String, Array[Byte]] {
      override def dataHandler[Meta]: Flow[EventPlusStreamMeta[String, Array[Byte], Meta], Meta, NotUsed] =
        FlowConverter.flowFor[String, Array[Byte], Meta](tupleFlow, partitionBy, new DefaultDataSinkExceptionHandler, 16)
    }
    new ManualOffsetManagementSubscriptionProvider(topic.name, Subscriptions.topics(topic.name), consumerSettings, businessFlow, offsetManager)
  }

  private def sendToTestProbe(testProbe: TestProbe)(key: String, value: Array[Byte]): Future[Done] = {
    val msg = stringDeserializer.deserialize("", value)
    testProbe.ref ! msg
    Future.successful(Done)
  }

  "ManualOffsetManagementSubscriptionProvider" should {
    "Pick up offsets from an offset provider" in {
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig =>
        val topic = KafkaTopic("testTopic")
        createCustomTopic(topic.name, partitions = 3)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"
        val probe = TestProbe()
        val offsetManager = new InMemoryOffsetManager()

        val record1 = "record 1"
        val record2 = "record 2"
        val record3 = "record 3"
        val record4 = "record 4"
        val record5 = "record 5"
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, "skipped 1", "skipped 1"))
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, "skipped 2", "skipped 2"))
        offsetManager.commit(new TopicPartition(topic.name, 0), 2) // Skip these 2 messages we've already seen

        val sub = testManualSubscriptionProvider(topic, kafkaBrokers = embeddedBroker, groupId = "subscription-test", sendToTestProbe(probe), offsetManager)
        sub.createSubscription(system).runWith(Sink.ignore)
        probe.expectNoMessage()

        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record1, record1))
        publishToKafka(new ProducerRecord[String, String](topic.name, 1, record2, record2))
        publishToKafka(new ProducerRecord[String, String](topic.name, 2, record3, record3))
        publishToKafka(new ProducerRecord[String, String](topic.name, 0, record4, record4))
        publishToKafka(new ProducerRecord[String, String](topic.name, 1, record5, record5))

        probe.expectMsgAllOf(10.seconds, record1, record2, record3, record4, record5)
        eventually {
          offsetManager.offsetFor(new TopicPartition(topic.name, 1)) shouldEqual Some(1)
          offsetManager.offsetFor(new TopicPartition(topic.name, 2)) shouldEqual Some(0)
        }
      }
    }
  }
}
