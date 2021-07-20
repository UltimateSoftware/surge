// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka

import akka.actor._
import akka.pattern._
import com.typesafe.config.{ Config, ConfigFactory }
import io.opentelemetry.api.trace.Tracer
import org.apache.kafka.common.TopicPartition
import org.slf4j.{ Logger, LoggerFactory }
import surge.internal.akka.ActorWithTracing
import surge.internal.akka.cluster.{ ActorHostAwareness, Shard }
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.config.TimeoutConfig
import surge.kafka.streams.HealthyActor.GetHealth
import surge.kafka.streams.{ HealthCheck, HealthCheckStatus, HealthyActor }
import surge.internal.tracing.TracedMessage

import java.time.Instant
import scala.concurrent.Future
import scala.concurrent.duration._

object KafkaPartitionShardRouterActor {
  private val config: Config = ConfigFactory.load()
  private val brokers = config.getString("kafka.brokers").split(",").toVector

  def props[Agg, Command, Event](
      partitionTracker: KafkaConsumerPartitionAssignmentTracker,
      partitioner: KafkaPartitioner[String],
      trackedTopic: KafkaTopic,
      regionCreator: PersistentActorRegionCreator[String],
      extractEntityId: PartialFunction[Any, String])(tracer: Tracer): Props = {

    // This producer is only used for determining partition assignments, not actually producing
    val producer = KafkaBytesProducer(brokers, trackedTopic, partitioner)
    Props(new KafkaPartitionShardRouterActor(partitionTracker, producer, regionCreator, extractEntityId)(tracer))
  }
  case object GetPartitionRegionAssignments
}

/**
 * The kafka partition shard router actor creates a mapping between Kafka partitions and assigned shards. The actor maintains state of which hosts are assigned
 * which partitions for a consumer group and routes messages as necessary to reach the shard where the message is destined. This is mostly used for ensuring in
 * Kafka Streams apps that an aggregate processing messages lives on the same node as the Kafka Streams processor for that aggregate. In the context of this
 * actor, shards and partitions for the topic being tracked maintain a 1:1 mapping.
 *
 * For messages destined to a shard owned by this instance of the shard router actor (based on the consumer group) this actor will create the shard if it does
 * not exist and forward the message to the shard - the shard itself is responsible for fine grained routing to the individual business entity. For messages
 * destined to a shard that lives remotely, the actor will forward to the shard router actor living on the host/port that is currently assigned the shard.
 *
 * This actor receives updates to the consumer group from an instance of the `KafkaConsumerStateTrackingActor`, which registers with Kafka Streams for state
 * updates and provides a view into which partitions are assigned to which application nodes. This actor simply registers to the
 * `KafkaConsumerStateTrackingActor` and updates its own internal mappings when it receives updates from the upstream partition tracker.
 *
 * @param partitionTracker
 *   The instance of a `KafkaConsumerStateTrackingActor` watching updates to the Kafka Streams consumer group and sending updates to any registered actors.
 * @param kafkaStateProducer
 *   An instance of a Kafka producer for the topic being tracked by this actor. This is not actually used to produce messages, but is used for the partitioning
 *   for messages in order to determine which partition a particular entity id is assigned to.
 * @param regionCreator
 *   An subclass of `TopicPartitionRegionCreator` used to create a new shard for messages destined to a local shard/partition that does not yet exist.
 * @param extractEntityId
 *   A partial function to extract an entity id from an incoming message. This actor can only handle routing
 */
class KafkaPartitionShardRouterActor(
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    kafkaStateProducer: KafkaProducerTrait[String, _],
    regionCreator: PersistentActorRegionCreator[String],
    extractEntityId: PartialFunction[Any, String])(implicit val tracer: Tracer)
    extends ActorWithTracing
    with Stash
    with ActorHostAwareness {

  import KafkaPartitionShardRouterActor._
  import context.dispatcher

  private val config = ConfigFactory.load()
  private val trackedTopic = kafkaStateProducer.topic
  private val enableDRStandbyInitial = config.getBoolean("surge.dr-standby-enabled")

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private case class ActorState(
      partitionAssignments: PartitionAssignments,
      partitionRegions: Map[Int, PartitionRegion],
      enableDRStandby: Boolean = enableDRStandbyInitial) {
    def partitionsToHosts: Map[Int, HostPort] = {
      partitionAssignments.topicPartitionsToHosts.map { case (tp, host) =>
        tp.partition() -> host
      }
    }

    def addRegion(partition: Int, region: ActorSelection, isLocal: Boolean): StatePlusRegion = {
      log.trace("RouterActor State adding partition region for partition {}", partition)
      val partitionRegion = PartitionRegion(partition, region, assignedSince = Instant.now, isLocal = isLocal)
      val newRegionsMap = partitionRegions + (partition -> partitionRegion)
      val newState = this.copy(partitionRegions = newRegionsMap)

      StatePlusRegion(newState, Some(partitionRegion))
    }

    def updatePartitionAssignments(partitionAssignments: PartitionAssignments): ActorState = {
      updatePartitionAssignments(partitionAssignments.partitionAssignments)
    }
    def updatePartitionAssignments(newAssignments: Map[HostPort, List[TopicPartition]]): ActorState = {
      val assignmentsForEventsTopic = newAssignments.map { case (hostPort, assignments) =>
        hostPort -> assignments.filter(_.topic() == trackedTopic.name)
      }

      val assignmentsWithChanges = partitionAssignments.update(assignmentsForEventsTopic)
      val revokedPartitions = assignmentsWithChanges.changes.revokedTopicPartitions
      val revokedPartitionNumbers = revokedPartitions.values.flatMap(_.map(_.partition())).toSeq

      val newPartitionRegions = partitionRegions.filterNot { case (region, _) =>
        revokedPartitionNumbers.contains(region)
      }
      // Stop any locally revoked regions running to preserve memory
      revokedPartitions.foreach { case (hostPort, topicPartitions) =>
        if (isHostPortThisNode(hostPort)) {
          topicPartitions.foreach { topicPartition =>
            val partition = topicPartition.partition()
            partitionRegions.get(partition).foreach { region =>
              log.info(s"Stopping local partition manager on $localHostname for partition $partition")
              region.regionManager ! PoisonPill
            }
          }
        } else if (topicPartitions.nonEmpty) {
          log.info(s"Disassociating partition region actors on $hostPort for partitions [${topicPartitions.mkString(", ")}]")
        }
      }

      this.copy(partitionAssignments = assignmentsWithChanges.assignments, partitionRegions = newPartitionRegions)
    }

    def initializeNewRegions(): ActorState = {
      val allTopicPartitions = partitionAssignments.partitionAssignments.values.flatten

      // If we're running in DR standby mode, don't automatically create new partition regions
      // Create them on demand when we need to send a message to them
      if (enableDRStandby) {
        this
      } else {
        allTopicPartitions.foldLeft(this) { case (stateAccum, topicPartition) =>
          partitionRegionFor(stateAccum, topicPartition.partition).state
        }
      }
    }
  }

  private val scheduledInitialize = context.system.scheduler.scheduleWithFixedDelay(0.seconds, 3.seconds)(() => initializeState())

  override def receive: Receive = uninitialized

  private def uninitialized: Receive = {
    case msg: PartitionAssignments => handle(msg)
    case _                         => stash()
  }

  // In standby mode, just follow updates to partition assignments and let Kafka streams index the aggregate state
  private def standbyMode(state: ActorState): Receive = {
    case msg if extractEntityId.isDefinedAt(msg) =>
      becomeActiveAndDeliverMessage(state, msg)
    case msg: PartitionAssignments     => handle(state, msg)
    case GetPartitionRegionAssignments => sender() ! state.partitionRegions
    case GetHealth =>
      sender() ! HealthCheck(name = "shard-router-actor", id = s"router-actor-$hashCode", status = HealthCheckStatus.UP)
  }

  private def initialized(state: ActorState): Receive = healthCheckReceiver(state).orElse {
    case msg: PartitionAssignments     => handle(state, msg)
    case msg: Terminated               => handle(state, msg)
    case GetPartitionRegionAssignments => sender() ! state.partitionRegions
    case msg if extractEntityId.isDefinedAt(msg) =>
      val entityId = extractEntityId(msg)
      activeSpan.log("extractEntityId", Map("entityId" -> entityId))
      deliverMessage(state, entityId, msg)
  }

  private def healthCheckReceiver(state: ActorState): Receive = { case GetHealth =>
    getHealthCheck(state).pipeTo(sender())
  }

  private def deliverMessage(state: ActorState, aggregateId: String, msg: Any): Unit = {
    partitionRegionFor(state, aggregateId) match {
      case Some(responsiblePartitionRegion) =>
        log.trace(s"Forwarding command envelope for aggregate $aggregateId to region ${responsiblePartitionRegion.regionManager.pathString}.")
        val tracedMsg = TracedMessage(msg, parentSpan = activeSpan)
        responsiblePartitionRegion.regionManager.forward(tracedMsg)
      case None =>
        log.error(s"Could not find a responsible partition region for $aggregateId.")
    }
  }

  private def partitionForAggregateId(aggregateId: String): Option[Int] = {
    kafkaStateProducer.partitionFor(aggregateId)
  }

  private def partitionRegionFor(state: ActorState, aggregateId: String): Option[PartitionRegion] = {
    partitionForAggregateId(aggregateId) match {
      case Some(partition) =>
        partitionRegionFor(state, partition).region
      case _ =>
        log.error(
          s"No partition calculated for aggregateId=$aggregateId - this is weird and " +
            s"either a bug in the code (partitioner incorrectly set) or an empty aggregateId (should not happen)")
        None
    }
  }

  private def partitionRegionFor(state: ActorState, partition: Int): StatePlusRegion = {
    val existingRegionOpt = state.partitionRegions.get(partition)

    existingRegionOpt.map(region => StatePlusRegion(state, Some(region))).getOrElse(newActorRegionForPartition(state, partition))
  }

  private case class StatePlusRegion(state: ActorState, region: Option[PartitionRegion])

  private def newActorRegionForPartition(state: ActorState, partition: Int): StatePlusRegion = {
    state.partitionsToHosts
      .get(partition)
      .map { hostPort =>
        val isLocal = isHostPortThisNode(hostPort)
        val newActorSelection = if (isLocal) {
          log.info(s"Creating partition region actor for partition {}", partition)

          val topicPartition = new TopicPartition(trackedTopic.name, partition)
          val region = regionCreator.regionFromTopicPartition(topicPartition)
          region.start()

          val shardProps = Shard.props(topicPartition.toString, region, extractEntityId)(tracer)

          val newActor = context.system.actorOf(shardProps)
          context.watch(newActor)
          context.actorSelection(newActor.path)
        } else {
          val remoteAddress = Address(akkaProtocol, context.system.name, hostPort.host, hostPort.port)
          log.info(s"Associating new remote router at $remoteAddress for partition $partition from $localHostname")

          val routerActorRemoteNode = self.path.toStringWithAddress(remoteAddress)
          context.actorSelection(routerActorRemoteNode)
        }

        state.addRegion(partition, newActorSelection, isLocal = isLocal)
      }
      .getOrElse {
        log.warn(
          s"Unable to find a partition assignment for partition {}.  This typically indicates unhealthiness " +
            s"in the Kafka streams consumer group.  If this warning continues, check the consumer group for the application to see if " +
            s"partitions for the aggregate state topic remain unassigned or if the Kafka Streams processor has stopped unexpectedly.",
          partition)
        StatePlusRegion(state, None)
      }
  }

  private def handle(state: ActorState, terminated: Terminated): Unit = {
    val terminatedActorPath = terminated.actor.path.toStringWithoutAddress
    val newPartitionRegions = state.partitionRegions.filterNot { case (_, actorSelection) =>
      val isMatchingActorPath = actorSelection.regionManager.pathString == terminatedActorPath

      if (isMatchingActorPath) {
        log.info(s"Partition region actor {} was terminated, not tracking it in state anymore", terminatedActorPath)
      }
      isMatchingActorPath
    }
    context.become(initialized(state.copy(partitionRegions = newPartitionRegions)))
  }

  private def handle(state: ActorState, partitionAssignments: PartitionAssignments): Unit = {
    log.info("RouterActor received new partition assignments")
    val newState = state.updatePartitionAssignments(partitionAssignments.partitionAssignments).initializeNewRegions()

    if (newState.enableDRStandby) {
      context.become(standbyMode(newState))
    } else {
      context.become(initialized(newState))
    }
  }

  private def becomeActiveAndDeliverMessage(state: ActorState, msg: Any): Unit = {
    log.info("Shard router transitioning from standby mode to active mode")
    val newState = state.copy(enableDRStandby = false).initializeNewRegions()
    context.become(initialized(newState))
    val entityId = extractEntityId(msg)
    activeSpan.log("extractEntityId", Map("entityId" -> entityId))
    deliverMessage(newState, entityId, msg)
  }

  private def handle(partitionAssignments: PartitionAssignments): Unit = {
    if (partitionAssignments.topicPartitionsToHosts.nonEmpty) {
      scheduledInitialize.cancel()
      unstashAll()
      log.debug(s"RouterActor initializing with partition assignments $partitionAssignments")

      val emptyState = ActorState(PartitionAssignments.empty, Map.empty)
      handle(emptyState, partitionAssignments)
    }
  }

  private def initializeState(): Unit = {
    log.debug(s"Initializing actor router with path = ${self.path}")
    partitionTracker.register(self)
  }

  private def getLocalPartitionRegionsHealth(partitionRegions: Map[Int, PartitionRegion]): Seq[Future[HealthCheck]] = {
    val localPartitionRegions = partitionRegions.filter { case (_, partitionRegion) => partitionRegion.isLocal }
    localPartitionRegions.map { case (_, partitionRegion) =>
      partitionRegion.regionManager.ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout * 2).mapTo[HealthCheck].recoverWith {
        case err: Throwable =>
          log.error(s"Failed to get partition region health check ${partitionRegion.regionManager.pathString}", err)
          Future.successful(
            HealthCheck(name = partitionRegion.regionManager.pathString, id = partitionRegion.regionManager.pathString, status = HealthCheckStatus.DOWN))
      }
    }.toSeq
  }

  private def getPartitionTrackerActorHealthCheck: Future[HealthCheck] = {
    partitionTracker.healthCheck().recoverWith { case err: Throwable =>
      log.error(s"Failed to get partition-tracker health check", err)
      Future.successful(
        HealthCheck(name = "partition-tracker", id = s"partition-tracker-actor-${partitionTracker.hashCode()}", status = HealthCheckStatus.DOWN))
    }
  }

  def getHealthCheck(state: ActorState): Future[HealthCheck] = {

    val localPartitionRegions = getLocalPartitionRegionsHealth(state.partitionRegions)
    val partitionTrackerHealthCheck = getPartitionTrackerActorHealthCheck

    Future.sequence(localPartitionRegions :+ partitionTrackerHealthCheck).map { shardHealthChecks =>
      HealthCheck(
        name = "shard-router-actor",
        id = trackedTopic.name,
        status = HealthCheckStatus.UP,
        details = Some(Map("enableDRStandby" -> enableDRStandbyInitial.toString)),
        components = Some(shardHealthChecks))
    }
  }
}
