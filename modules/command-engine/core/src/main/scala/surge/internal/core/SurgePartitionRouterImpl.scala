// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.core

import akka.actor._
import akka.cluster.sharding.external.ExternalShardAllocationStrategy
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings }
import akka.kafka.ConsumerSettings
import akka.kafka.cluster.sharding.KafkaClusterSharding
import akka.pattern.ask
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import org.slf4j.LoggerFactory
import play.api.libs.json.JsValue
import surge.core.{ Ack, Controllable, KafkaProducerActor, SurgePartitionRouter }
import surge.health.HealthSignalBusTrait
import surge.internal.akka.kafka.{ KafkaConsumerPartitionAssignmentTracker, KafkaShardingClassicMessageExtractor }
import surge.internal.config.TimeoutConfig
import surge.internal.health.{ HealthCheck, HealthCheckStatus, HealthyActor, HealthyComponent }
import surge.internal.persistence
import surge.internal.persistence.{ BusinessLogic, PersistentActor }
import surge.internal.tracing.RoutableMessage
import surge.kafka.streams._
import surge.kafka.{ KafkaPartitionShardRouterActor, KafkaSecurityConfigurationImpl, PersistentActorRegionCreator }

import java.util.Properties
import java.util.regex.Pattern
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.jdk.CollectionConverters._

private[surge] final class SurgePartitionRouterImpl(
    config: Config,
    system: ActorSystem,
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    businessLogic: BusinessLogic,
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    regionCreator: PersistentActorRegionCreator[String],
    signalBus: HealthSignalBusTrait,
    isAkkaClusterEnabled: Boolean)
    extends SurgePartitionRouter
    with HealthyComponent {
  implicit val executionContext: ExecutionContext = system.dispatcher
  private val log = LoggerFactory.getLogger(getClass)

  override val actorRegion: ActorRef = {
    if (isAkkaClusterEnabled) {
      createAkkaCluster()
    } else {
      createShardRouter()
    }
  }

  log.info(s"Shard region: $actorRegion")

  private def createShardRouter(): ActorRef = {
    val shardRouterProps = KafkaPartitionShardRouterActor.props(
      config,
      partitionTracker,
      businessLogic.partitioner,
      businessLogic.kafka.stateTopic,
      regionCreator,
      RoutableMessage.extractEntityId)(businessLogic.tracer)

    val routerActorName = s"${businessLogic.aggregateName}RouterActor"
    val shardRouter = system.actorOf(shardRouterProps, name = routerActorName)
    shardRouter
  }

  private def initializeKafkaConsumerSettings(groupId: String): ConsumerSettings[String, Array[Byte]] = {
    val securityRelatedProps = new Properties()
    val securityHelper = new KafkaSecurityConfigurationImpl(config)
    securityHelper.configureSecurityProperties(securityRelatedProps)

    ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(config.getString("kafka.brokers"))
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperties(securityRelatedProps.asScala.toMap)
  }

  private def createAkkaCluster(): ActorRef = {
    val groupId = businessLogic.kafka.streamsApplicationId
    val consumerSettings = initializeKafkaConsumerSettings(groupId)

    val regionF = KafkaClusterSharding(system)
      .messageExtractorNoEnvelope(
        timeout = 10.seconds,
        topic = businessLogic.kafka.stateTopic.name,
        entityIdExtractor = (msg: RoutableMessage) => msg.aggregateId,
        settings = consumerSettings)
      .map(messageExtractor => {
        val classicMessageExtractor = new KafkaShardingClassicMessageExtractor[RoutableMessage](
          kafkaPartitions = messageExtractor.kafkaPartitions,
          entityIdExtractor = (msg: RoutableMessage) => msg.aggregateId)
        val aggregateIdToKafkaProducerActor = KafkaProducerActor.createWithActorSelection(
          actorSystem = system,
          metrics = businessLogic.metrics,
          businessLogic = businessLogic,
          signalBus = signalBus,
          numberOfPartitions = messageExtractor.kafkaPartitions)

        val aggregateMetrics = PersistentActor.createMetrics(businessLogic.metrics, businessLogic.aggregateName)
        val sharedResources = persistence.PersistentEntitySharedResources(aggregateIdToKafkaProducerActor, aggregateMetrics, aggregateKafkaStreamsImpl)
        ClusterSharding(system).start(
          typeName = groupId,
          entityProps = PersistentActor.props(businessLogic, sharedResources, config, None),
          settings = ClusterShardingSettings(system),
          messageExtractor = classicMessageExtractor,
          allocationStrategy = new ExternalShardAllocationStrategy(system, groupId),
          handOffStopMessage = PoisonPill)
      })
    // FIXME This could be fixed once we drop existing router
    Await.result(regionF, 5.seconds)
  }

  override def restartSignalPatterns(): Seq[Pattern] = Seq(Pattern.compile("kafka.fatal.error"))

  override def healthCheck(): Future[HealthCheck] = {
    actorRegion
      .ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout * 3) // * 3 since this rolls up 2 additional health checks below
      .mapTo[HealthCheck]
      .recoverWith { case err: Throwable =>
        log.error(s"Failed to get router-actor health check", err)
        Future.successful(HealthCheck(name = "router-actor", id = s"router-actor-${actorRegion.hashCode}", status = HealthCheckStatus.DOWN))
      }
  }

  private[surge] override val controllable: Controllable = new Controllable {
    override def start(): Future[Ack] = {
      // TODO explicit start/stop for router actor
      //implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PartitionRouter.askTimeout)
      //actorRegion.ask(ActorLifecycleManagerActor.Start).mapTo[Ack].andThen(registrationCallback())
      Future.successful(Ack())
    }

    override def restart(): Future[Ack] = {
      for {
        _ <- stop()
        started <- start()
      } yield {
        started
      }
    }

    override def stop(): Future[Ack] = {
      // TODO explicit start/stop for router actor
      //implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PartitionRouter.askTimeout)
      //actorRegion.ask(ActorLifecycleManagerActor.Stop).mapTo[Ack]
      Future.successful(Ack())
    }

    override def shutdown(): Future[Ack] = stop()
  }
}
