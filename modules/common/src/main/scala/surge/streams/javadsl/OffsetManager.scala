// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.streams.javadsl

import akka.Done
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._

abstract class OffsetManager extends surge.streams.OffsetManager {
  private implicit val executionContext: ExecutionContext = ExecutionContext.global
  override def getOffsets(topics: Set[TopicPartition]): Future[Map[TopicPartition, Long]] = Future {
    getOffsets(topics.asJava).asScala.toMap
  }
  override def commit(topicPartition: TopicPartition, offset: Long): Future[Done] = Future {
    commitSync(topicPartition, offset)
  }.map(_ => Done)

  def getOffsets(topics: java.util.Set[TopicPartition]): java.util.Map[TopicPartition, Long]
  def commitSync(topicPartition: TopicPartition, offset: Long): Unit
}
