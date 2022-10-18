// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.kafka

import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.MessageExtractor
import surge.kafka.KafkaPartitionProvider

final class KafkaShardingClassicMessageExtractor[M](val partitionProvider: KafkaPartitionProvider, val kafkaPartitions: Int, entityIdExtractor: M => String)
    extends MessageExtractor {
  override def entityId(message: Any): String = entityIdExtractor(message.asInstanceOf[M])

  override def entityMessage(message: Any): Any = message.asInstanceOf[M]

  override def shardId(message: Any): String = {
    val id = message match {
      case ShardRegion.StartEntity(entityId) => entityId
      case _                                 => entityId(message)
    }
    val partitionNumber = partitionProvider.partitionForKey(id, kafkaPartitions)
    partitionNumber.toString
  }
}
