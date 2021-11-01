// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.kafka

import akka.actor.{ Actor, ActorRef, Props }
import akka.cluster.Cluster
import akka.cluster.sharding.external.ExternalShardAllocation
import org.slf4j.LoggerFactory
import surge.internal.akka.cluster.ActorHostAwareness
import surge.kafka.PartitionAssignments

import scala.util.{ Failure, Success }

object KafkaClusterShardingRebalanceListener {
  def props(stateTrackingActor: ActorRef, topic: String, groupId: String): Props = {
    Props(new KafkaClusterShardingRebalanceListener(stateTrackingActor, topic, groupId))
  }
}

class KafkaClusterShardingRebalanceListener(stateTrackingActor: ActorRef, topic: String, groupId: String) extends Actor with ActorHostAwareness {
  import context.dispatcher

  private val log = LoggerFactory.getLogger(getClass)
  private val shardAllocationClient = ExternalShardAllocation(context.system).clientFor(groupId)
  private val address = Cluster(context.system).selfMember.address

  override def preStart(): Unit = {
    stateTrackingActor ! KafkaConsumerStateTrackingActor.Register(self)
  }

  override def receive: Receive = { case assignments: PartitionAssignments =>
    val assignmentsForStateTopic = assignments.partitionAssignments.map { case (hostPort, assignments) =>
      hostPort -> assignments.filter(_.topic() == topic)
    }

    val updatedTopicPartitions = assignmentsForStateTopic.collect {
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

      case Failure(ex) =>
        log.error("A failure occurred while updating cluster shards", ex)
    }
  }
}
