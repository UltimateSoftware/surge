// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.akka.streams.kafka

import java.util.Properties

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerConfigExtension }
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, Deserializer, StringDeserializer }
import surge.config.TimeoutConfig
import surge.scala.core.kafka.KafkaSecurityConfiguration

import scala.collection.JavaConverters._

object KafkaConsumer extends KafkaSecurityConfiguration {
  private val config: Config = ConfigFactory.load()
  private val defaultBrokers = config.getString("kafka.brokers")

  def defaultConsumerSettings(actorSystem: ActorSystem, groupId: String): ConsumerSettings[String, Array[Byte]] = {
    implicit val stringDeserializer: StringDeserializer = new StringDeserializer()
    implicit val byteArrayDeserializer: ByteArrayDeserializer = new ByteArrayDeserializer()

    consumerSettings[String, Array[Byte]](actorSystem, groupId)
  }

  def consumerSettings[Key, Value](
    actorSystem: ActorSystem,
    groupId: String,
    brokers: String = defaultBrokers)(implicit keyDeserializer: Deserializer[Key], valueDeserializer: Deserializer[Value]): ConsumerSettings[Key, Value] = {

    val baseSettings = ConsumerSettings[Key, Value](actorSystem, Some(keyDeserializer), Some(valueDeserializer))

    val securityProps = new Properties()
    configureSecurityProperties(securityProps)

    baseSettings
      .withBootstrapServers(brokers)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString.toLowerCase)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, TimeoutConfig.Kafka.consumerSessionTimeout.toMillis.toString)
      .withProperty(ConsumerConfigExtension.LEAVE_GROUP_ON_CLOSE_CONFIG, TimeoutConfig.debugTimeoutEnabled.toString)
      .withProperties(securityProps.asScala.toMap)
  }
}
