// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams

import akka.Done
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future

/**
 * An offset manager is responsible for maintaining the offsets for a data pipeline. The offset manager can either use Kafka (the normal and recommended
 * default) or a manual offset storage external to Kafka. To store offsets externally to Kafka you can implement a custom OffsetManager, which just needs to
 * provide ways to commit offsets during normal processing and reload saved offsets on startup & consumer group rebalance events. Offsets are always committed
 * after a message is processed regardless of the offset manager, resulting in at-least-once processing anywhere using a Surge DataSource or EventSource.
 */
trait OffsetManager {
  def getOffsets(topics: Set[TopicPartition]): Future[Map[TopicPartition, Long]]
  def commit(topicPartition: TopicPartition, offset: Long): Future[Done]
}

/**
 * The default Kafka offset manager does nothing since we use the Akka Streams committer flow internally to manage offsets automatically rather than explicitly
 * managing offsets external to Kafka.
 */
class DefaultKafkaOffsetManager extends OffsetManager {
  override def commit(topicPartition: TopicPartition, offset: Long): Future[Done] = Future.successful(Done)
  override def getOffsets(topics: Set[TopicPartition]): Future[Map[TopicPartition, Long]] = Future.successful(Map.empty)
}
