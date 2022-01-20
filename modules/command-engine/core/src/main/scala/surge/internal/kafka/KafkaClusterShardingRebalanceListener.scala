// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import akka.actor.{ Actor, ActorRef, Address, Props }
import akka.cluster.Cluster
import akka.cluster.sharding.external.ExternalShardAllocation
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import surge.core.KafkaProducerActor
import surge.internal.akka.cluster.ActorHostAwareness
import surge.internal.akka.kafka.KafkaConsumerStateTrackingActor
import surge.kafka.{ HostPort, PartitionAssignments }

import scala.util.{ Failure, Success }

object KafkaClusterShardingRebalanceListener {
  def props(stateTrackingActor: ActorRef, partitionToKafkaProducerActor: Int => KafkaProducerActor, topic: String, groupId: String): Props = {
    Props(new KafkaClusterShardingRebalanceListener(stateTrackingActor, partitionToKafkaProducerActor, topic, groupId))
  }
}

class KafkaClusterShardingRebalanceListener(
    stateTrackingActor: ActorRef,
    partitionToKafkaProducerActor: Int => KafkaProducerActor,
    topic: String,
    groupId: String)
    extends Actor
    with ActorHostAwareness {
  import context.dispatcher

  private case class StatePlusRegion(state: ActorState, kafkaProducerRegion: Option[KafkaProducerActor])

  private case class ActorState(
      partitionAssignments: PartitionAssignments,
      partitionKafkaProducerRegions: Map[Int, KafkaProducerActor],
      joinedSeedNodes: List[Address]) {
    def partitionsToHosts: Map[Int, HostPort] = {
      partitionAssignments.topicPartitionsToHosts.map { case (tp, host) =>
        tp.partition() -> host
      }
    }

    def updatePartitionAssignments(partitionAssignments: PartitionAssignments): ActorState = {
      updatePartitionAssignments(partitionAssignments.partitionAssignments)
    }

    def updatePartitionAssignments(newAssignments: Map[HostPort, List[TopicPartition]]): ActorState = {
      val assignmentsForTopic = newAssignments.map { case (hostPort, assignments) =>
        hostPort -> assignments.filter(_.topic() == topic)
      }

      val assignmentsWithChanges = partitionAssignments.update(assignmentsForTopic)
      val assignmentsOnThisNode =
        assignmentsWithChanges.assignments.copy(partitionAssignments = assignmentsWithChanges.assignments.partitionAssignments.collect {
          case (hostPort, topicPartitions) if isHostPortThisNode(hostPort) => (hostPort, topicPartitions)
        })

      val revokedPartitions = assignmentsWithChanges.changes.revokedTopicPartitions
      val revokedPartitionNumbers = revokedPartitions.values.flatMap(_.map(_.partition())).toSeq

      val newPartitionRegions = partitionKafkaProducerRegions.filterNot { case (region, _) =>
        revokedPartitionNumbers.contains(region)
      }
      // Stop any locally revoked kafka producer regions running to preserve memory
      revokedPartitions.foreach { case (hostPort, topicPartitions) =>
        if (isHostPortThisNode(hostPort)) {
          topicPartitions.foreach { topicPartition =>
            val partition = topicPartition.partition()
            partitionKafkaProducerRegions.get(partition).foreach { region =>
              log.info(s"Stopping partition kafka producer on $localHostname for partition $partition")
              region.terminate()
            }
          }
        } else if (topicPartitions.nonEmpty) {
          log.info(s"Disassociating partition kafka producers on $hostPort for partitions [${topicPartitions.mkString(", ")}]")
        }
      }

      this.copy(partitionAssignments = assignmentsOnThisNode, partitionKafkaProducerRegions = newPartitionRegions)
    }

    def initializeNewKafkaProducerRegions(): ActorState = {
      val allTopicPartitions = partitionAssignments.partitionAssignments.values.flatten

      allTopicPartitions.foldLeft(this) { case (stateAccum, topicPartition) =>
        partitionRegionFor(stateAccum, topicPartition.partition).state
      }
    }

    private def partitionRegionFor(state: ActorState, partition: Int): StatePlusRegion = {
      val existingRegionOpt = state.partitionKafkaProducerRegions.get(partition)

      existingRegionOpt.map(region => StatePlusRegion(state, Some(region))).getOrElse(newKafkaProducerRegionForPartition(state, partition))
    }

    private def newKafkaProducerRegionForPartition(state: ActorState, partition: Int): StatePlusRegion = {
      state.partitionsToHosts
        .get(partition)
        .map { _ =>
          val kafkaProducerRegion = partitionToKafkaProducerActor(partition)
          kafkaProducerRegion.controllable.start()
          state.addRegionForPartition(partition, kafkaProducerRegion)
        }
        .getOrElse {
          log.warn(s"Unable to find a partition assignment for partition $partition")
          StatePlusRegion(state, None)
        }
    }

    private def addRegionForPartition(partition: Int, kafkaProducerRegion: KafkaProducerActor): StatePlusRegion = {
      val newRegionsMap = partitionKafkaProducerRegions + (partition -> kafkaProducerRegion)
      val newState = this.copy(partitionKafkaProducerRegions = newRegionsMap)
      StatePlusRegion(newState, Some(kafkaProducerRegion))
    }

  }

  private val log = LoggerFactory.getLogger(getClass)
  private val shardAllocationClient = ExternalShardAllocation(context.system).clientFor(groupId)
  private val address = Cluster(context.system).selfMember.address

  override def preStart(): Unit = {
    stateTrackingActor ! KafkaConsumerStateTrackingActor.Register(self)
  }

  override def receive: Receive = uninitialized

  def uninitialized: Receive = { case assignments: PartitionAssignments =>
    handle(assignments)
  }

  def initialized(state: ActorState): Receive = { case assignments: PartitionAssignments =>
    handle(state, assignments)
  }

  private def handle(partitionAssignments: PartitionAssignments): Unit = {
    if (partitionAssignments.topicPartitionsToHosts.nonEmpty) {
      val emptyState = ActorState(PartitionAssignments.empty, Map.empty, List.empty)
      handle(emptyState, partitionAssignments)
    }
  }

  private def handle(state: ActorState, partitionAssignments: PartitionAssignments): Unit = {
    val cluster = Cluster.get(actorSystem)
    val joinedSeedNodes = state.joinedSeedNodes
    val seedNodesOfPartitionAssignment =
      partitionAssignments.partitionAssignments.keys.map(hostPort => cluster.selfAddress.copy(host = Some(hostPort.host), port = Some(hostPort.port))).toList
    val newSeedNodes = seedNodesOfPartitionAssignment.filter(address => !isAddressThisNode(address) && !joinedSeedNodes.contains(address)).sorted

    val updatedJoinSeedNodes = joinedSeedNodes.filter(seedNode => seedNodesOfPartitionAssignment.contains(seedNode)) ++ newSeedNodes

    val currentClusterAddress = cluster.selfAddress
    val isLowestAddressNode = !updatedJoinSeedNodes.flatMap(_.host).exists(host => host < currentClusterAddress.host.getOrElse("127.0.0.1"))
    if (isLowestAddressNode && joinedSeedNodes.isEmpty) {
      cluster.joinSeedNodes(currentClusterAddress +: newSeedNodes)
    } else if (newSeedNodes.nonEmpty) {
      cluster.joinSeedNodes(newSeedNodes)
    }

    val newStateWithPartitionAssignments = state.updatePartitionAssignments(partitionAssignments).copy(joinedSeedNodes = updatedJoinSeedNodes)

    val updatedTopicPartitions = newStateWithPartitionAssignments.partitionAssignments.partitionAssignments.collect {
      case (hostPort, topicPartitions) if isHostPortThisNode(hostPort) => topicPartitions
    }.flatten

    val updates = shardAllocationClient.updateShardLocations(updatedTopicPartitions.map { tp =>
      val shardId = tp.partition().toString
      // the Kafka partition number becomes the akka shard id
      (shardId, address)
    }.toMap)

    updates.onComplete {
      case Success(_) =>
        log.info("Completed groupId '{}' assignment of topic partitions to cluster member '{}': [{}]", groupId, address, updatedTopicPartitions.mkString(","))
        val newState = newStateWithPartitionAssignments.initializeNewKafkaProducerRegions()
        context.become(initialized(newState))
      case Failure(ex) =>
        log.error("A failure occurred while updating cluster shards", ex)
    }
  }

}
