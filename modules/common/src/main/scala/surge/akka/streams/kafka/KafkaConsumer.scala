// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.akka.streams.kafka

import java.util.Properties

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{ Committer, Consumer }
import akka.kafka.{ CommitterSettings, ConsumerMessage, ConsumerSettings, Subscriptions }
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, Source }
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerConfigExtension }
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, Deserializer, StringDeserializer }
import org.slf4j.{ Logger, LoggerFactory }
import surge.config.TimeoutConfig
import surge.scala.core.kafka.{ KafkaSecurityConfiguration, KafkaTopic }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }

trait KafkaConsumerTrait[Key, Value] {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  def actorSystem: ActorSystem

  private def defaultCommitterSettings = CommitterSettings(actorSystem)

  private val defaultParallelism = 10

  def createCommittableSource(
    topic: KafkaTopic,
    consumerSettings: ConsumerSettings[Key, Value]): Source[ConsumerMessage.CommittableMessage[Key, Value], Consumer.Control] = {
    Consumer.committableSource(consumerSettings, Subscriptions.topics(topic.name))
  }

  def commitOffsetSinkAndRun(
    source: Source[ConsumerMessage.Committable, Consumer.Control],
    committerSettings: CommitterSettings = defaultCommitterSettings)(implicit materializer: Materializer): DrainingControl[Done] = {
    source
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
  }

  def streamAndCommitOffsets(
    topic: KafkaTopic,
    business: (Key, Value) ⇒ Future[Any],
    parallelism: Int = defaultParallelism,
    consumerSettings: ConsumerSettings[Key, Value],
    committerSettings: CommitterSettings = defaultCommitterSettings)(implicit mat: Materializer, ec: ExecutionContext): Consumer.DrainingControl[Done] = {
    val source = createCommittableSource(topic, consumerSettings)

    val flow = source.mapAsync(parallelism) { msg ⇒
      business(msg.record.key, msg.record.value)
        .map(_ ⇒ msg.committableOffset)
        .recover {
          case e ⇒
            log.error(
              "An exception was thrown by the business logic! The stream will be stopped and must be manually restarted.  If you hit this often " +
                "you can try the **experimental** managed stream implementation by setting `surge.use-new-consumer = true` in your configuration.",
              e)
            throw e
        }
    }

    commitOffsetSinkAndRun(flow, committerSettings)
  }

}

case class DefaultKafkaConsumer()(implicit val actorSystem: ActorSystem) extends KafkaConsumerTrait[String, Array[Byte]]
case class KafkaConsumer[Key, Value]()(implicit val actorSystem: ActorSystem) extends KafkaConsumerTrait[Key, Value]
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
