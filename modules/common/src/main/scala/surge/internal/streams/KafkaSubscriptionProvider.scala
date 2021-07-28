// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{AutoSubscription, CommitterSettings, ConsumerSettings, Subscription}
import akka.stream.scaladsl.{Flow, Source}
import com.typesafe.config.Config
import io.opentelemetry.api.trace.Tracer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.LoggerFactory
import surge.internal.akka.cluster.ActorSystemHostAwareness
import surge.internal.kafka.HostAwarenessConfig
import surge.streams.{DataHandler, OffsetManager}

import java.util.UUID
import scala.concurrent.duration.Duration

trait KafkaSubscriptionProvider[Key, Value] {
  protected val config: Config

  val tracer: Tracer

  private lazy val reuseConsumerId = config.getBoolean("surge.kafka-reuse-consumer-id")
  // Set this uniquely per manager actor so that restarts of the Kafka stream don't cause a rebalance of the consumer group
  val clientId = s"surge-event-source-managed-consumer-${UUID.randomUUID()}"

  private class HostAware(val actorSystem: ActorSystem) extends ActorSystemHostAwareness {
    def hostName: String = localHostname
    def port: Int = localPort
  }
  protected def createConsumerSettings[K, V](actorSystem: ActorSystem, baseConsumerSettings: ConsumerSettings[K, V]): ConsumerSettings[K, V] = {
    val hostAware = new HostAware(actorSystem)
    val consumerSettingsWithHost = baseConsumerSettings
      .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, PartitionAssignorConfig.assignorClassName(config))
      .withProperty(HostAwarenessConfig.HOST_CONFIG, hostAware.hostName)
      .withProperty(HostAwarenessConfig.PORT_CONFIG, hostAware.port.toString)
      .withStopTimeout(Duration.Zero)

    if (reuseConsumerId) {
      consumerSettingsWithHost.withClientId(clientId).withGroupInstanceId(clientId)
    } else {
      consumerSettingsWithHost
    }
  }

  def businessFlow: DataHandler[Key, Value]
  def createSubscription(actorSystem: ActorSystem): Source[Done, Consumer.Control]
}

class KafkaOffsetManagementSubscriptionProvider[Key, Value](
    override val config: Config,
    topicName: String,
    subscription: Subscription,
    baseConsumerSettings: ConsumerSettings[Key, Value],
    override val businessFlow: DataHandler[Key, Value])(override val tracer: Tracer)
    extends KafkaSubscriptionProvider[Key, Value] {

  private val log = LoggerFactory.getLogger(getClass)
  private val committerMaxBatch = config.getLong("surge.kafka-event-source.committer.max-batch")
  private val committerMaxInterval = config.getDuration("surge.kafka-event-source.committer.max-interval")
  private val committerParallelism = config.getInt("surge.kafka-event-source.committer.parallelism")

  private val kafkaFlow = KafkaStreamManager.wrapBusinessFlow(businessFlow.dataHandler)(tracer)
  override def createSubscription(actorSystem: ActorSystem): Source[Done, Consumer.Control] = {
    val committerSettings =
      CommitterSettings(actorSystem).withMaxBatch(committerMaxBatch).withMaxInterval(committerMaxInterval).withParallelism(committerParallelism)
    val consumerSettings = createConsumerSettings(actorSystem, baseConsumerSettings)
    log.debug("Creating Kafka source for topic {} with client id {}", Seq(topicName, clientId): _*)
    Consumer.committableSource(consumerSettings, subscription).via(kafkaFlow).map(_.committableOffset).via(Committer.flow(committerSettings))
  }
}

class ManualOffsetManagementSubscriptionProvider[Key, Value](
    override val config: Config,
    topicName: String,
    subscription: AutoSubscription,
    baseConsumerSettings: ConsumerSettings[Key, Value],
    override val businessFlow: DataHandler[Key, Value],
    offsetManager: OffsetManager,
    maxPartitions: Int = 10)(override val tracer: Tracer)
    extends KafkaSubscriptionProvider[Key, Value] {
  private val log = LoggerFactory.getLogger(getClass)
  private val kafkaFlow = KafkaStreamManager.wrapBusinessFlow(businessFlow.dataHandler)(tracer)
  override def createSubscription(actorSystem: ActorSystem): Source[Done, Consumer.Control] = {
    val consumerSettings = createConsumerSettings(actorSystem, baseConsumerSettings)
    log.debug("Creating Kafka source for topic {} with client id {}", Seq(topicName, clientId): _*)
    val committerFlow = Flow[CommittableOffset].mapAsync(1) { offset =>
      offsetManager.commit(offset.partitionOffset.key.topicPartition, offset.partitionOffset.offset)
    }
    Consumer
      .committablePartitionedManualOffsetSource(consumerSettings, subscription, offsetManager.getOffsets)
      .flatMapMerge(maxPartitions, _._2)
      .via(kafkaFlow)
      .map(_.committableOffset)
      .via(committerFlow)
  }
}
