// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.kafka

import java.util.Properties

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.serialization.Deserializer
import surge.kafka.KafkaSecurityConfiguration

import scala.jdk.CollectionConverters._

object AkkaKafkaConsumer extends KafkaSecurityConfiguration {
  private val config: Config = ConfigFactory.load()
  private val defaultBrokers = config.getString("kafka.brokers")
  private val consumerSessionTimeout = config.getDuration("surge.kafka-event-source.consumer.session-timeout")
  private val autoOffsetReset = config.getString("surge.kafka-event-source.consumer.auto-offset-reset")

  def consumerSettings[Key, Value](actorSystem: ActorSystem, groupId: String, brokers: String = defaultBrokers)(
      implicit keyDeserializer: Deserializer[Key],
      valueDeserializer: Deserializer[Value]): ConsumerSettings[Key, Value] = {

    val baseSettings = ConsumerSettings[Key, Value](actorSystem, Some(keyDeserializer), Some(valueDeserializer))

    val securityProps = new Properties()
    configureSecurityProperties(securityProps)

    baseSettings
      .withBootstrapServers(brokers)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString.toLowerCase)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.toLowerCase)
      .withProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumerSessionTimeout.toMillis.toString)
      .withProperties(securityProps.asScala.toMap)
  }
}
