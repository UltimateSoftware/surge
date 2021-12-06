// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.{ ActorRef, ActorSelection, ActorSystem, NoSerializationVerificationNeeded, PoisonPill, Props }
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Headers
import org.slf4j.LoggerFactory
import surge.health.{ HealthSignalBusAware, HealthSignalBusTrait }
import surge.internal.SurgeModel
import surge.internal.akka.actor.{ ActorLifecycleManagerActor, ManagedActorRef }
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.config.TimeoutConfig
import surge.internal.kafka.{ KTableLagCheckerImpl, KafkaProducerActorImpl, PartitionerHelper }
import surge.internal.kafka.KafkaProducerActorImpl.ShutdownProducer
import surge.kafka.streams._
import surge.kafka.{ KafkaAdminClient, KafkaProducerTrait }
import surge.metrics.{ MetricInfo, Metrics, Timer }

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object KafkaProducerActor {
  private val dispatcherName: String = "kafka-publisher-actor-dispatcher"

  //scalastyle:off parameter.number
  def apply(
      actorSystem: ActorSystem,
      assignedPartition: TopicPartition,
      metrics: Metrics,
      businessLogic: SurgeModel[_, _, _, _],
      kStreams: AggregateStateStoreKafkaStreams[_],
      partitionTracker: KafkaConsumerPartitionAssignmentTracker,
      signalBus: HealthSignalBusTrait,
      config: Config,
      kafkaProducerOverride: Option[KafkaProducerTrait[String, Array[Byte]]] = None): KafkaProducerActor = {

    val brokers = config.getString("kafka.brokers").split(",").toVector
    val adminClient = KafkaAdminClient(config, brokers)

    val kafkaProducerProps = Props(
      new KafkaProducerActorImpl(
        assignedPartition = assignedPartition,
        metrics = metrics,
        businessLogic,
        lagChecker = new KTableLagCheckerImpl(kStreams.applicationId, adminClient),
        partitionTracker = partitionTracker,
        signalBus = signalBus,
        config = config,
        kafkaProducerOverride = kafkaProducerOverride)).withDispatcher(dispatcherName)

    new KafkaProducerActor(
      publisherActor = ActorLifecycleManagerActor.manage(
        actorSystem = actorSystem,
        managedActorProps = kafkaProducerProps,
        componentName = s"producer-actor-${assignedPartition.toString}",
        stopMessageAdapter = Some(() => ShutdownProducer)),
      metrics,
      businessLogic.aggregateName,
      assignedPartition,
      signalBus)
  }

  def createFromPartitionNumber(
      actorSystem: ActorSystem,
      metrics: Metrics,
      businessLogic: SurgeModel[_, _, _, _],
      kStreams: AggregateStateStoreKafkaStreams[_],
      partitionTracker: KafkaConsumerPartitionAssignmentTracker,
      signalBus: HealthSignalBusTrait,
      config: Config,
      kafkaProducerOverride: Option[KafkaProducerTrait[String, Array[Byte]]] = None): Int => KafkaProducerActor = partitionNumber => {

    val assignedPartition = new TopicPartition(businessLogic.kafka.stateTopic.name, partitionNumber)

    val brokers = config.getString("kafka.brokers").split(",").toVector
    val adminClient = KafkaAdminClient(config, brokers)

    val kafkaProducerProps = Props(
      new KafkaProducerActorImpl(
        assignedPartition = assignedPartition,
        metrics = metrics,
        businessLogic,
        lagChecker = new KTableLagCheckerImpl(kStreams.applicationId, adminClient),
        partitionTracker = partitionTracker,
        signalBus = signalBus,
        config = config,
        kafkaProducerOverride = kafkaProducerOverride)).withDispatcher(dispatcherName)

    new KafkaProducerActor(
      ActorLifecycleManagerActor.manage(
        actorSystem = actorSystem,
        managedActorProps = kafkaProducerProps,
        componentName = s"producer-actor-${assignedPartition.toString}",
        actorLifecycleName = Some(s"producer-actor-${assignedPartition.toString}"),
        stopMessageAdapter = Some(() => ShutdownProducer)),
      metrics,
      businessLogic.aggregateName,
      assignedPartition,
      signalBus)
  }

  def createWithActorSelection(
      actorSystem: ActorSystem,
      metrics: Metrics,
      businessLogic: SurgeModel[_, _, _, _],
      signalBus: HealthSignalBusTrait,
      numberOfPartitions: Int): String => KafkaProducerActor = (aggregateId: String) => {

    val partitionNumber = PartitionerHelper.partitionForKey(aggregateId, numberOfPartitions)
    val assignedPartition = new TopicPartition(businessLogic.kafka.stateTopic.name, partitionNumber)

    // FIXME This could be an ActorSelection once we drop the old router
    val timeout = 5.seconds
    val publisherActor = Await.result(actorSystem.actorSelection(s"user/producer-actor-${assignedPartition.toString}").resolveOne(timeout), timeout)

    new KafkaProducerActor(ManagedActorRef(publisherActor), metrics, businessLogic.aggregateName, assignedPartition, signalBus)
  }

  sealed trait PublishResult extends NoSerializationVerificationNeeded
  case object PublishSuccess extends PublishResult
  case class PublishFailure(t: Throwable) extends PublishResult
  case class MessageToPublish(key: String, value: Array[Byte], headers: Headers)
}

/**
 * A stateful producer actor responsible for publishing all states + events for aggregates that belong to a particular state topic partition. The state
 * maintained by this producer actor is a list of aggregate ids which are considered "in flight". "In flight" is determined by keeping track of the offset this
 * actor publishes to for each aggregate id as messages are published to Kafka and listening to updates of the downstream Kafka Streams consumer as it makes
 * progress through the topic. As a state is published, this actor remembers the aggregate id and offset the state for that aggregate is. When the Kafka Streams
 * consumer processes the state (saving it to a KTable) it notifies the MetadataHandler with the offset of the most recently processed message. The
 * MetadataHandler will publish an event KafkaPartitionMetadata This producer actor subscribes to the KafkaPartitionMetadata events to get the last processed
 * offset and marks any aggregates in the "in flight" state as up to date if their offset is less than or equal to the last processed offset.
 *
 * When an aggregate actor wants to initialize, it must first ask this stateful producer if the state for that aggregate is up to date in the Kafka Streams
 * state store KTable. The stateful producer is able to determine this by looking at the aggregates with states that are in flight - if any are in flight for an
 * aggregate, the state in the KTable is not up to date and initialization of that actor should be delayed.
 *
 * On initialization of the stateful producer, it emits an empty "flush" record to the Kafka state topic. The flush record is for an empty aggregate id, but is
 * used to ensure on initial startup that there were no remaining aggregate states that were in flight, since the newly created producer cannot initialize with
 * the knowledge of everything that was published previously.
 *
 * @param publisherActor
 *   ManagedActorRef holding the underlying publisher actor-ref used to batch and publish messages to Kafka
 * @param metrics
 *   Metrics provider to use for recording internal metrics to
 * @param aggregateName
 *   The name of the aggregate this publisher is responsible for
 * @param assignedPartition
 *   TopicPartition
 * @param signalBus
 *   HealthSignalBusTrait
 */
class KafkaProducerActor(
    publisherActor: ManagedActorRef,
    metrics: Metrics,
    aggregateName: String,
    val assignedPartition: TopicPartition,
    override val signalBus: HealthSignalBusTrait)
    extends HealthyComponent
    with HealthSignalBusAware {
  private implicit val executionContext: ExecutionContext = ExecutionContext.global
  private val log = LoggerFactory.getLogger(getClass)

  def publish(
      aggregateId: String,
      state: KafkaProducerActor.MessageToPublish,
      events: Seq[KafkaProducerActor.MessageToPublish]): Future[KafkaProducerActor.PublishResult] = {
    log.trace(s"Publishing state for {} {}", Seq(aggregateName, state.key): _*)
    implicit val ec: ExecutionContext = ExecutionContext.global
    implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PublisherActor.publishTimeout)
    (publisherActor.ref ? KafkaProducerActorImpl.Publish(eventsToPublish = events, state = state)).mapTo[KafkaProducerActor.PublishResult]
  }
  def terminate(): Unit = {
    publisherActor.ref ! PoisonPill
  }

  private val isAggregateStateCurrentTimer: Timer = metrics.timer(
    MetricInfo(
      s"surge.${aggregateName.toLowerCase()}.is-aggregate-current-timer",
      "Average time in milliseconds taken to check if a particular aggregate is up to date in the KTable",
      tags = Map("aggregate" -> aggregateName)))
  def isAggregateStateCurrent(aggregateId: String): Future[Boolean] = {
    implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PublisherActor.aggregateStateCurrentTimeout)
    isAggregateStateCurrentTimer.timeFuture {
      (publisherActor.ref ? KafkaProducerActorImpl.IsAggregateStateCurrent(aggregateId)).mapTo[Boolean]
    }
  }

  def healthCheck(): Future[HealthCheck] = {
    publisherActor.ref
      .ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout)
      .mapTo[HealthCheck]
      .recoverWith { case err: Throwable =>
        log.error(s"Failed to get publisher-actor health check", err)
        Future.successful(HealthCheck(name = "publisher-actor", id = aggregateName, status = HealthCheckStatus.DOWN))
      }(ExecutionContext.global)
  }

  private def registrationCallback(): PartialFunction[Try[Ack], Unit] = {
    case Success(_) =>
      signalBus
        .register(
          control = controllable,
          componentName = s"kafka-producer-actor-${assignedPartition.topic()}-${assignedPartition.partition()}",
          restartSignalPatterns = restartSignalPatterns())
        .onComplete {
          case Failure(exception) =>
            log.error(s"$getClass registration failed", exception)
          case Success(_) =>
            log.debug(s"$getClass registration succeeded")
        }
    case Failure(error) =>
      log.error(s"Unable to register $getClass for supervision", error)
  }

  private[surge] override val controllable: Controllable = new Controllable {

    override def start(): Future[Ack] = {
      publisherActor.start().andThen(registrationCallback())
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
      publisherActor.stop()
    }

    override def shutdown(): Future[Ack] = stop()
  }
}
