// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.kafka

import java.nio.ByteBuffer
import java.util

import com.ultimatesoftware.scala.core.kafka.HostPort
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment
import org.apache.kafka.clients.consumer.{ ConsumerGroupMetadata, ConsumerPartitionAssignor, RangeAssignor }
import org.apache.kafka.common.{ Cluster, Configurable }

import scala.collection.JavaConverters._
import scala.util.Try

class HostAwareRangeAssignor extends RangeAssignor with Configurable {
  private var hostPort: Option[HostPort] = None

  override def configure(configs: util.Map[String, _]): Unit = {
    for {
      host ← configs.asScala.get(HostAwarenessConfig.HOST_CONFIG).flatMap(host ⇒ Try(host.toString).toOption)
      port ← configs.asScala.get(HostAwarenessConfig.PORT_CONFIG).flatMap(port ⇒ Try(port.toString.toInt).toOption)
    } {
      hostPort = Some(HostPort(host, port))
    }
  }

  override def subscriptionUserData(topics: util.Set[String]): ByteBuffer = {
    SubscriptionInfo(hostPort = hostPort).encode
  }

  override def assign(metadata: Cluster, groupSubscription: ConsumerPartitionAssignor.GroupSubscription): ConsumerPartitionAssignor.GroupAssignment = {
    val withNoMetadata = super.assign(metadata, groupSubscription)

    val hostPortMappings = groupSubscription.groupSubscription().asScala.flatMap {
      case (key, subscription) ⇒
        SubscriptionInfo.decode(subscription.userData()).flatMap(_.hostPort).toList.flatMap { hostPort ⇒
          val assignedPartitions = Option(withNoMetadata.groupAssignment().get(key)).map(_.partitions().asScala.toSet).getOrElse(Set.empty)
          assignedPartitions.map(_ -> hostPort)
        }
    }
    val assignmentInfo = AssignmentInfo(hostPortMappings.toList)
    val assignmentUserData = assignmentInfo.encode

    val assignmentsWithMetadata = withNoMetadata.groupAssignment().asScala.map {
      case (key, assignment) ⇒
        key -> new Assignment(assignment.partitions(), assignmentUserData)
    }

    new ConsumerPartitionAssignor.GroupAssignment(assignmentsWithMetadata.asJava)
  }

  override def onAssignment(assignment: ConsumerPartitionAssignor.Assignment, metadata: ConsumerGroupMetadata): Unit = {
    AssignmentInfo.decode(assignment.userData()).foreach { assignmentInfo ⇒
      HostAssignmentTracker.updateState(assignmentInfo.assignedPartitions)
    }
  }
}

object HostAwarenessConfig {
  val HOST_CONFIG: String = "application.host"
  val PORT_CONFIG: String = "application.port"
}
