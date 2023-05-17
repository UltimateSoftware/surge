// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.kafka

import scala.util.hashing.MurmurHash3

trait KafkaPartitionProvider {
  def partitionForKey(partitionByString: String, numberOfPartitions: Int): Int = math.abs(MurmurHash3.stringHash(partitionByString) % numberOfPartitions)
}
sealed trait KafkaPartitionerBase[Key] extends KafkaPartitionProvider {
  def optionalPartitionBy: Option[Key => String]
}

object NoPartitioner {
  def apply[A]: NoPartitioner[A] = new NoPartitioner[A]
}
final class NoPartitioner[Key] extends KafkaPartitionerBase[Key] {
  override def optionalPartitionBy: Option[Key => String] = None
}

trait KafkaPartitioner[Key] extends KafkaPartitionerBase[Key] {
  override final def optionalPartitionBy: Option[Key => String] = Some(partitionBy)
  def partitionBy: Key => String
}

object StringIdentityPartitioner {
  val instance: StringIdentityPartitioner = StringIdentityPartitioner()
  def apply(): StringIdentityPartitioner = new StringIdentityPartitioner
}
final class StringIdentityPartitioner extends KafkaPartitioner[String] {
  override def partitionBy: String => String = identity[String]
}

object PartitionStringUpToColon {
  val instance: PartitionStringUpToColon = PartitionStringUpToColon()
  def apply(): PartitionStringUpToColon = new PartitionStringUpToColon
}
final class PartitionStringUpToColon extends KafkaPartitioner[String] {
  override def partitionBy: String => String = { str =>
    str.takeWhile(_ != ':')
  }
}
