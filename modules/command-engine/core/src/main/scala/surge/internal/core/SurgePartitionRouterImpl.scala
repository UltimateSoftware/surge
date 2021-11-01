// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.core

import akka.actor._
import akka.cluster.sharding.external.ExternalShardAllocationStrategy
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import akka.kafka.ConsumerSettings
import akka.kafka.cluster.sharding.KafkaClusterSharding
import akka.pattern.ask
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.slf4j.LoggerFactory
import play.api.libs.json.JsValue
import surge.core.{Ack, Controllable, KafkaProducerActor, SurgePartitionRouter}
import surge.health.HealthSignalBusTrait
import surge.internal.SurgeModel
import surge.internal.akka.kafka.{KafkaConsumerPartitionAssignmentTracker, KafkaShardingClassicMessageExtractor}
import surge.internal.config.TimeoutConfig
import surge.internal.persistence.{PersistentActor, RoutableMessage}
import surge.kafka.{KafkaPartitionShardRouterActor, PersistentActorRegionCreator}
import surge.kafka.streams._

import java.util.regex.Pattern
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.languageFeature.postfixOps
import scala.util.{Failure, Success, Try}

private[surge] final class SurgePartitionRouterImpl(
    config: Config,
    system: ActorSystem,
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    businessLogic: SurgeModel[_, _, _, _],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    regionCreator: PersistentActorRegionCreator[String],
    signalBus: HealthSignalBusTrait)
    extends SurgePartitionRouter
    with HealthyComponent
    with Controllable {
  implicit val executionContext: ExecutionContext = system.dispatcher
  private val log = LoggerFactory.getLogger(getClass)

  private val isAkkaClusterEnabled: Boolean = Try(config.getBoolean("surge.akka.cluster.enabled")).getOrElse(false)

  override val actorRegion: ActorRef = {
    if (isAkkaClusterEnabled) {
      val groupId = businessLogic.kafka.streamsApplicationId
      val consumerSettings = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
        .withBootstrapServers(config.getString("kafka.brokers"))
        .withGroupId(groupId)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withStopTimeout(0.seconds)

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
          val aggregateIdToKafkaProducerActor = KafkaProducerActor.create(
            actorSystem = system,
            metrics = businessLogic.metrics,
            businessLogic = businessLogic,
            partitionTracker = partitionTracker,
            kStreams = aggregateKafkaStreamsImpl,
            signalBus = signalBus,
            config = config,
            numberOfPartitions = messageExtractor.kafkaPartitions)

          ClusterSharding(system).start(
            typeName = groupId,
            entityProps = PersistentActor.props(aggregateIdToKafkaProducerActor, businessLogic, signalBus, aggregateKafkaStreamsImpl, config),
            settings = ClusterShardingSettings(system),
            messageExtractor = classicMessageExtractor,
            allocationStrategy = new ExternalShardAllocationStrategy(system, groupId),
            handOffStopMessage = PoisonPill)
        })
      // FIXME
      Await.result(regionF, 5.seconds)
    } else {
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
  }

  log.info(s"Shard region: $actorRegion")

  override def start(): Future[Ack] = {
    // TODO explicit start/stop for router actor
    //implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PartitionRouter.askTimeout)
    //actorRegion.ask(ActorLifecycleManagerActor.Start).mapTo[Ack].andThen(registrationCallback())
    Future.successful(Ack())
  }

  override def stop(): Future[Ack] = {
    // TODO explicit start/stop for router actor
    //implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PartitionRouter.askTimeout)
    //actorRegion.ask(ActorLifecycleManagerActor.Stop).mapTo[Ack]
    Future.successful(Ack())
  }

  override def shutdown(): Future[Ack] = stop()

  override def restartSignalPatterns(): Seq[Pattern] = Seq(Pattern.compile("kafka.fatal.error"))

  override def restart(): Future[Ack] = {
    for {
      _ <- stop()
      started <- start()
    } yield {
      started
    }
  }

  override def healthCheck(): Future[HealthCheck] = {
    actorRegion
      .ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout * 3) // * 3 since this rolls up 2 additional health checks below
      .mapTo[HealthCheck]
      .recoverWith { case err: Throwable =>
        log.error(s"Failed to get router-actor health check", err)
        Future.successful(HealthCheck(name = "router-actor", id = s"router-actor-${actorRegion.hashCode}", status = HealthCheckStatus.DOWN))
      }
  }

  private def registrationCallback(): PartialFunction[Try[Ack], Unit] = {
    case Success(_) =>
      val registrationResult = signalBus.register(control = this, componentName = "router-actor", restartSignalPatterns())

      registrationResult.onComplete {
        case Failure(exception) =>
          log.error(s"$getClass registration failed", exception)
        case Success(_) =>
          log.debug(s"$getClass registration succeeded")
      }
    case Failure(error) =>
      log.error(s"Unable to register $getClass for supervision", error)
  }

}
