// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import java.nio.ByteBuffer
import java.util

import org.apache.kafka.clients.consumer.{ ConsumerGroupMetadata, ConsumerPartitionAssignor, CooperativeStickyAssignor }
import org.apache.kafka.common.Cluster

import scala.jdk.CollectionConverters._

class HostAwareCooperativeStickyAssignor extends CooperativeStickyAssignor with HostAwarenessConfig {
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
