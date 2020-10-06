// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.kafka

import java.nio.ByteBuffer

import com.ultimatesoftware.scala.core.kafka.HostPort
import com.ultimatesoftware.scala.core.utils.JsonFormats
import org.apache.kafka.common.TopicPartition
import play.api.libs.json.{ Format, JsValue, Json }

private object ByteBufferUtils {
  def toString(byteBuffer: ByteBuffer): String = {
    val tempArray = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(tempArray)
    new String(tempArray)
  }
}

object SubscriptionInfo {
  implicit val format: Format[SubscriptionInfo] = Json.format
  def decode(byteBuffer: ByteBuffer): Option[SubscriptionInfo] = {
    val jsonString = ByteBufferUtils.toString(byteBuffer)
    Json.parse(jsonString).asOpt[SubscriptionInfo]
  }
}
case class SubscriptionInfo(hostPort: Option[HostPort]) {
  def toJson: JsValue = Json.toJson(this)
  def encode: ByteBuffer = ByteBuffer.wrap(toJson.toString().getBytes())
}

object AssignmentInfo {
  implicit val topicPartitionFormat: Format[TopicPartition] = JsonFormats.jsonFormatterFromJackson[TopicPartition]
  implicit val format: Format[AssignmentInfo] = Json.format
  def decode(byteBuffer: ByteBuffer): Option[AssignmentInfo] = {
    val jsonString = ByteBufferUtils.toString(byteBuffer)
    Json.parse(jsonString).asOpt[AssignmentInfo]
  }
}
case class AssignmentInfo(partitionAssignments: List[(TopicPartition, HostPort)]) {
  def assignedPartitions: Map[TopicPartition, HostPort] = partitionAssignments.toMap
  def toJson: JsValue = Json.toJson(this)
  def encode: ByteBuffer = ByteBuffer.wrap(toJson.toString().getBytes())
}
