// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.core

import java.util.regex.Pattern
import akka.actor._
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import akka.cluster.sharding.external.ExternalShardAllocationStrategy
import akka.kafka.ConsumerSettings
import akka.kafka.cluster.sharding.KafkaClusterSharding
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.slf4j.LoggerFactory
import play.api.libs.json.JsValue
import surge.core.{Ack, Controllable, KafkaProducerActor, SurgePartitionRouter}
import surge.health.HealthSignalBusTrait
import surge.internal.{SurgeModel, persistence}
import surge.internal.akka.actor.ActorLifecycleManagerActor
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.config.TimeoutConfig
import surge.internal.kafka.KafkaShardingMessageExtractor
import surge.internal.persistence.{PersistentActor, RoutableMessage}
import surge.kafka.streams.{AggregateStateStoreKafkaStreams, HealthCheck, HealthCheckStatus, HealthyActor, HealthyComponent}
import surge.kafka.{KafkaPartitionShardRouterActor, PersistentActorRegionCreator}

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

  private val partitionToKafkaProducerActor = KafkaProducerActor.create(
    actorSystem = system,
    metrics = businessLogic.metrics,
    businessLogic = businessLogic,
    partitionTracker = partitionTracker,
    kStreams = aggregateKafkaStreamsImpl,
    signalBus = signalBus,
    config = config)

  // FIXME
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(config.getString("kafka.brokers"))
    .withGroupId(businessLogic.kafka.streamsApplicationId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withStopTimeout(0.seconds)

  val regionF = KafkaClusterSharding(system).messageExtractorNoEnvelope(
    timeout = 10.seconds,
    topic = businessLogic.kafka.stateTopic.name,
    entityIdExtractor = (msg: RoutableMessage) => msg.aggregateId,
    settings = consumerSettings
  ).map(messageExtractor => {
    val classicMessageExtractor = new KafkaShardingMessageExtractor[RoutableMessage](
      kafkaPartitions = messageExtractor.kafkaPartitions,
      entityIdExtractor = (msg: RoutableMessage) => msg.aggregateId
    )
    system.log.info("Message extractor created. Initializing sharding")
    ClusterSharding(system).start(
      typeName = businessLogic.kafka.streamsApplicationId, // FIXME
      entityProps = PersistentActor.props(partitionToKafkaProducerActor, businessLogic, signalBus, aggregateKafkaStreamsImpl, config),
      settings = ClusterShardingSettings(system),
      messageExtractor = classicMessageExtractor,
      allocationStrategy = new ExternalShardAllocationStrategy(system, businessLogic.kafka.streamsApplicationId), // FIXME
      handOffStopMessage = PoisonPill
    )
  })

  override val actorRegion: ActorRef = Await.result(regionF, 5.seconds)

  log.info(s"Shard region: $actorRegion")

//  private val shardRouterProps = KafkaPartitionShardRouterActor.props(
//    config,
//    partitionTracker,
//    businessLogic.partitioner,
//    businessLogic.kafka.stateTopic,
//    regionCreator,
//    RoutableMessage.extractEntityId)(businessLogic.tracer)
//
//  private val routerActorName = s"${businessLogic.aggregateName}RouterActor"
//  private val shardRouter = system.actorOf(shardRouterProps, name = routerActorName)
//  override val actorRegion: ActorRef = shardRouter

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
