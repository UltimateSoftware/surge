// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import java.nio.ByteBuffer
import java.util

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment
import org.apache.kafka.clients.consumer.{ ConsumerGroupMetadata, ConsumerPartitionAssignor, RangeAssignor }
import org.apache.kafka.common.{ Cluster, Configurable }
import surge.kafka.HostPort

import scala.jdk.CollectionConverters._
import scala.util.Try

class HostAwareRangeAssignor extends RangeAssignor with HostAwarenessConfig {
  override def subscriptionUserData(topics: util.Set[String]): ByteBuffer = {
    SubscriptionInfo(hostPort = hostPort).encode
  }

  override def assign(metadata: Cluster, groupSubscription: ConsumerPartitionAssignor.GroupSubscription): ConsumerPartitionAssignor.GroupAssignment = {
    val withNoMetadata = super.assign(metadata, groupSubscription)
    val assignmentsWithMetadata = addMetadataToAssignment(withNoMetadata, groupSubscription)
    new ConsumerPartitionAssignor.GroupAssignment(assignmentsWithMetadata.asJava)
  }

  override def onAssignment(assignment: ConsumerPartitionAssignor.Assignment, metadata: ConsumerGroupMetadata): Unit = {
    handleOnAssignment(assignment, metadata)
  }
}

object HostAwarenessConfig {
  val HOST_CONFIG: String = "application.host"
  val PORT_CONFIG: String = "application.port"
}
trait HostAwarenessConfig extends Configurable {
  protected var hostPort: Option[HostPort] = None

  override def configure(configs: util.Map[String, _]): Unit = {
    for {
      host <- configs.asScala.get(HostAwarenessConfig.HOST_CONFIG).flatMap(host => Try(host.toString).toOption)
      port <- configs.asScala.get(HostAwarenessConfig.PORT_CONFIG).flatMap(port => Try(port.toString.toInt).toOption)
    } {
      hostPort = Some(HostPort(host, port))
    }
  }

  protected def addMetadataToAssignment(
    groupAssignment: ConsumerPartitionAssignor.GroupAssignment,
    groupSubscription: ConsumerPartitionAssignor.GroupSubscription): Map[String, Assignment] = {

    val hostPortMappings = groupSubscription.groupSubscription().asScala.flatMap {
      case (key, subscription) =>
        SubscriptionInfo.decode(subscription.userData()).flatMap(_.hostPort).toList.flatMap { hostPort =>
          val assignedPartitions = Option(groupAssignment.groupAssignment().get(key)).map(_.partitions().asScala.toSet).getOrElse(Set.empty)
          assignedPartitions.map(_ -> hostPort)
        }
    }
    val assignmentInfo = AssignmentInfo(hostPortMappings.toList)
    val assignmentUserData = assignmentInfo.encode

    val assignmentsWithMetadata = groupAssignment.groupAssignment().asScala.map {
      case (key, assignment) =>
        key -> new Assignment(assignment.partitions(), assignmentUserData)
    }
    assignmentsWithMetadata.toMap
  }

  protected def handleOnAssignment(assignment: ConsumerPartitionAssignor.Assignment, metadata: ConsumerGroupMetadata): Unit = {
    val decodedUserData = Option(assignment.userData()).flatMap(AssignmentInfo.decode)
    decodedUserData.foreach { assignmentInfo =>
      HostAssignmentTracker.updateState(assignmentInfo.assignedPartitions)
    }
  }
}
