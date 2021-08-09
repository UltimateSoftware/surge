// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka

import com.typesafe.config.Config

import java.util.Properties
import org.apache.kafka.clients.admin.{ Admin, ListOffsetsOptions, OffsetSpec }
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo
import org.apache.kafka.clients.consumer.{ ConsumerConfig, OffsetAndMetadata }
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

case class LagInfo(currentOffsetPosition: Long, endOffsetPosition: Long) {
  val offsetLag: Long = Math.max(0, endOffsetPosition - currentOffsetPosition)
}

object KafkaAdminClient {
  def apply(config: Config, brokers: Seq[String]): KafkaAdminClient = {
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
    new KafkaSecurityConfigurationImpl(config).configureSecurityProperties(p)
    apply(p)
  }

  def apply(props: Properties): KafkaAdminClient = {
    new KafkaAdminClient(props)
  }
}

class KafkaAdminClient(props: Properties) {
  private val client = Admin.create(props)

  def consumerGroupOffsets(groupName: String): Map[TopicPartition, OffsetAndMetadata] = {
    client.listConsumerGroupOffsets(groupName).partitionsToOffsetAndMetadata().get().asScala.toMap
  }

  def endOffsets(partitions: List[TopicPartition], options: ListOffsetsOptions = new ListOffsetsOptions()): Map[TopicPartition, ListOffsetsResultInfo] = {
    client.listOffsets(partitions.map(tp => tp -> OffsetSpec.latest()).toMap.asJava).all().get().asScala.toMap
  }

  def consumerLag(
      groupName: String,
      topicPartitions: List[TopicPartition],
      listOffsetsOptions: ListOffsetsOptions = new ListOffsetsOptions()): Map[TopicPartition, LagInfo] = {
    val cgOffsets = consumerGroupOffsets(groupName)
    val tpEndOffsets = endOffsets(topicPartitions, listOffsetsOptions)

    tpEndOffsets.map { case (topicPartition, endOffsetMeta) =>
      val cgOffset = cgOffsets.getOrElse(topicPartition, new OffsetAndMetadata(0L))
      val lagInfo = LagInfo(cgOffset.offset(), endOffsetMeta.offset())
      topicPartition -> lagInfo
    }
  }
}
