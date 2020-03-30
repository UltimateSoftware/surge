// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.kafka

import java.nio.ByteBuffer
import java.util

import com.ultimatesoftware.scala.core.kafka.HostPort
import org.apache.kafka.clients.consumer.{ ConsumerGroupMetadata, ConsumerPartitionAssignor, RangeAssignor }
import org.apache.kafka.common.Configurable

import scala.collection.JavaConverters._
import scala.util.Try

class HostAwareRangeAssignor extends RangeAssignor with Configurable {
  private var hostPort: Option[HostPort] = None

  override def configure(configs: util.Map[String, _]): Unit = {
    for {
      host ← configs.asScala.get(HostAwarenessConfig.HOST_CONFIG).flatMap(host ⇒ Try(host.toString).toOption)
      port ← configs.asScala.get(HostAwarenessConfig.PORT_CONFIG).flatMap(host ⇒ Try(host.toString.toInt).toOption)
    } {
      hostPort = Some(HostPort(host, port))
    }
  }

  override def subscriptionUserData(topics: util.Set[String]): ByteBuffer = {
    hostPort.map(_.toByteBuffer).orNull
  }

  override def onAssignment(assignment: ConsumerPartitionAssignor.Assignment, metadata: ConsumerGroupMetadata): Unit = {
    val hostPortOpt = HostPort.fromByteBuffer(assignment.userData())
    hostPortOpt.foreach { meta ⇒
      HostAssignmentTracker.updateState(meta, assignment.partitions().asScala.toList)
    }
  }
}

object HostAwarenessConfig {
  val HOST_CONFIG: String = "application.host"
  val PORT_CONFIG: String = "application.port"
}
