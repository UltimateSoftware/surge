// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka

import java.nio.ByteBuffer

import org.apache.kafka.common.TopicPartition
import play.api.libs.json.{ Format, Json }

object HostPort {
  implicit val format: Format[HostPort] = Json.format

  def fromByteBuffer(byteBuffer: ByteBuffer): Option[HostPort] = {
    Json.parse(byteBuffer.array()).asOpt[HostPort]
  }
}
final case class HostPort(host: String, port: Int) {
  override def toString: String = s"$host:$port"
  def toByteBuffer: ByteBuffer = ByteBuffer.wrap(Json.toBytes(Json.toJson(this)))
}

object PartitionAssignmentChanges {
  private def missingPartitionMappings(
    startingAssignments: Map[HostPort, List[TopicPartition]],
    endingAssignments: Map[HostPort, List[TopicPartition]]): Map[HostPort, List[TopicPartition]] = {
    startingAssignments.map {
      case (hostPort, topicPartitions) =>
        val newTopicPartitionsForHost = endingAssignments.getOrElse(hostPort, List.empty)
        val removedTopicPartitionsForHost = topicPartitions.diff(newTopicPartitionsForHost)
        hostPort -> removedTopicPartitionsForHost
    }
  }

  def diff(
    currentAssignments: Map[HostPort, List[TopicPartition]],
    newAssignments: Map[HostPort, List[TopicPartition]]): PartitionAssignmentChanges = {

    val revokedPartitions = missingPartitionMappings(currentAssignments, newAssignments)
    val addedPartitions = missingPartitionMappings(newAssignments, currentAssignments)

    PartitionAssignmentChanges(revokedTopicPartitions = revokedPartitions, addedTopicPartitions = addedPartitions)
  }
}
final case class PartitionAssignmentChanges(revokedTopicPartitions: Map[HostPort, List[TopicPartition]], addedTopicPartitions: Map[HostPort, List[TopicPartition]])

final case class PartitionAssignmentsWithChanges(assignments: PartitionAssignments, changes: PartitionAssignmentChanges)

object PartitionAssignments {
  def empty: PartitionAssignments = PartitionAssignments(Map.empty)
}
final case class PartitionAssignments(partitionAssignments: Map[HostPort, List[TopicPartition]]) {
  def topicPartitionsToHosts: Map[TopicPartition, HostPort] = {
    partitionAssignments.flatMap {
      case (hostPort, topicPartitions) =>
        topicPartitions.map(tp => tp -> hostPort)
    }
  }

  def update(newPartitionAssignments: Map[HostPort, List[TopicPartition]]): PartitionAssignmentsWithChanges = {
    val changes = PartitionAssignmentChanges.diff(partitionAssignments, newPartitionAssignments)
    val assignments = this.copy(partitionAssignments = newPartitionAssignments)
    PartitionAssignmentsWithChanges(assignments = assignments, changes = changes)
  }

}
