// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.MessageExtractor
import org.apache.kafka.common.utils.Utils

final class KafkaShardingMessageExtractor[M](val kafkaPartitions: Int, entityIdExtractor: M => String) extends MessageExtractor {
  override def entityId(message: Any): String = entityIdExtractor(message.asInstanceOf[M])

  override def entityMessage(message: Any): Any = message.asInstanceOf[M]

  override def shardId(message: Any): String = {
    val id = message match {
      case ShardRegion.StartEntity(entityId) => entityId
      case _                                 => entityId(message)
    }
    // simplified version of Kafka's `DefaultPartitioner` implementation
    val partition = org.apache.kafka.common.utils.Utils
      .toPositive(Utils.murmur2(id.getBytes())) % kafkaPartitions
    partition.toString
  }
}