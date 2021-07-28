// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.kafka

import java.util.Properties
import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.serialization.Deserializer
import surge.kafka.KafkaSecurityConfiguration

import scala.jdk.CollectionConverters._

class AkkaKafkaConsumer(override val config: Config) extends KafkaSecurityConfiguration {
  def consumerSettings[Key, Value](actorSystem: ActorSystem, groupId: String, brokers: String, autoOffsetReset: String)(
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
      .withProperties(securityProps.asScala.toMap)
  }
}
