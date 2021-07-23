// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import java.nio.ByteBuffer
import org.apache.kafka.common.TopicPartition
import play.api.libs.json.{ Format, JsNumber, JsObject, JsPath, JsString, JsValue, Json, Reads, Writes }
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._
import surge.kafka.HostPort

import scala.util.Try

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
  implicit val topicPartitionReads: Reads[TopicPartition] = ((JsPath \ "topic").read[String].and((JsPath \ "partition").read[Int]))(new TopicPartition(_, _))
  implicit val topicPartitionWrites: Writes[TopicPartition] = (o: TopicPartition) =>
    JsObject(Map("topic" -> JsString(o.topic()), "partition" -> JsNumber(o.partition())))
  implicit val topicPartitionFormat: Format[TopicPartition] = Format(topicPartitionReads, topicPartitionWrites)

  implicit val format: Format[AssignmentInfo] = Json.format
  def decode(byteBuffer: ByteBuffer): Option[AssignmentInfo] = {
    val jsonString = ByteBufferUtils.toString(byteBuffer)
    Try(Json.parse(jsonString)).toOption.flatMap(_.asOpt[AssignmentInfo])
  }
}
case class AssignmentInfo(partitionAssignments: List[(TopicPartition, HostPort)]) {
  def assignedPartitions: Map[TopicPartition, HostPort] = partitionAssignments.toMap
  def toJson: JsValue = Json.toJson(this)
  def encode: ByteBuffer = ByteBuffer.wrap(toJson.toString().getBytes())
}
