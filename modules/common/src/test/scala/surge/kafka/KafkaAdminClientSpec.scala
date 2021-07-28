// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka

import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ Serializer, StringSerializer }
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpec

class KafkaAdminClientSpec extends AnyWordSpec with Matchers with EmbeddedKafka with Eventually {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(10, Millis)))

  private val defaultConfig = ConfigFactory.load()

  "KafkaAdminClient" should {
    "Correctly calculate consumer lag" in {
      withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { implicit actualConfig =>
        implicit val stringSerializer: Serializer[String] = new StringSerializer
        val topic = KafkaTopic("test-topic")
        createCustomTopic(topic.name, partitions = 3)
        val partition0 = new TopicPartition(topic.name, 0)
        val partition1 = new TopicPartition(topic.name, 1)
        val partition2 = new TopicPartition(topic.name, 2)
        val embeddedBroker = s"localhost:${actualConfig.kafkaPort}"
        val client = KafkaAdminClient(defaultConfig, Seq(embeddedBroker))

        val consumerGroupName = "test-consumer-group"
        val consumer = KafkaStringConsumer(defaultConfig, Seq(embeddedBroker), UltiKafkaConsumerConfig(consumerGroupName), Map.empty).consumer
        consumer.subscribe(java.util.Arrays.asList(topic.name))

        publishToKafka(new ProducerRecord[String, String](topic.name, 0, "", "initial value"))
        client.consumerLag(consumerGroupName, List(partition0)) shouldEqual Map(partition0 -> LagInfo(0L, 1L))

        client.consumerLag("non-existent-consumer-group", List(partition0)) shouldEqual Map(partition0 -> LagInfo(0L, 1L))

        publishToKafka(new ProducerRecord[String, String](topic.name, 1, "", "partition 1 value"))
        publishToKafka(new ProducerRecord[String, String](topic.name, 1, "", "partition 1 second value"))
        client.consumerLag(consumerGroupName, List(partition0, partition1, partition2)) shouldEqual Map(
          partition0 -> LagInfo(0L, 1L),
          partition1 -> LagInfo(0L, 2L),
          partition2 -> LagInfo(0L, 0L))

        eventually {
          consumer.poll(java.time.Duration.ofMillis(50))
          consumer.commitSync()

          client.consumerLag(consumerGroupName, List(partition0, partition1, partition2)) shouldEqual Map(
            partition0 -> LagInfo(1L, 1L),
            partition1 -> LagInfo(2L, 2L),
            partition2 -> LagInfo(0L, 0L))
        }
      }
    }
  }
}
