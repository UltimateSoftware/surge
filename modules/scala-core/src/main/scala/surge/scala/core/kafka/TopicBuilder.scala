// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.kafka

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig

import scala.collection.JavaConverters._

case class TopicBuilder(name: String, partitions: Option[Int] = None, replicas: Option[Short] = None, extraConfig: Map[String, String] = Map.empty) {
  private val defaultNumPartitions: Int = 3
  private val defaultReplicas: Short = 1

  def toAdminTopic: NewTopic = {
    val numberPartitions = partitions.getOrElse(defaultNumPartitions)
    val replicationFactor = replicas.getOrElse(defaultReplicas)
    new NewTopic(name, numberPartitions, replicationFactor).configs(extraConfig.asJava)
  }
  def build(): NewTopic = toAdminTopic

  def partitions(partitions: Int): TopicBuilder = this.copy(partitions = Some(partitions))
  def replicas(replicas: Short): TopicBuilder = this.copy(replicas = Some(replicas))
  def config(key: String, value: String): TopicBuilder = {
    val newConfigs = extraConfig + (key -> value)
    this.copy(extraConfig = newConfigs)
  }
}

object TopicBuilder {
  def name(name: String): TopicBuilder = TopicBuilder(name)

  def kafkaTopic(kafkaTopic: KafkaTopic): TopicBuilder = {
    val cleanupPolicy = if (kafkaTopic.compacted) {
      TopicConfig.CLEANUP_POLICY_COMPACT
    } else {
      TopicConfig.CLEANUP_POLICY_DELETE
    }
    val topicConfiguration = Map[String, String](
      TopicConfig.CLEANUP_POLICY_CONFIG -> cleanupPolicy)
    TopicBuilder(name = kafkaTopic.name, partitions = kafkaTopic.numberPartitionsOverride, extraConfig = topicConfiguration)
  }
}
