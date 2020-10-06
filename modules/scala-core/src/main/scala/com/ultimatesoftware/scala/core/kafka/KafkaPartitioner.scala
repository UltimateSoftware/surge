// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.kafka

sealed trait KafkaPartitionerBase[Key] {
  def optionalPartitionBy: Option[Key ⇒ String]
}

object NoPartitioner {
  def apply[A]: NoPartitioner[A] = new NoPartitioner[A]
}
final class NoPartitioner[Key] extends KafkaPartitionerBase[Key] {
  override def optionalPartitionBy: Option[Key ⇒ String] = None
}

trait KafkaPartitioner[Key] extends KafkaPartitionerBase[Key] {
  override final def optionalPartitionBy: Option[Key ⇒ String] = Some(partitionBy)
  def partitionBy: Key ⇒ String
}

object StringIdentityPartitioner extends KafkaPartitioner[String] {
  override def partitionBy: String ⇒ String = identity[String]
}

object PartitionStringUpToColon extends KafkaPartitioner[String] {
  override def partitionBy: String ⇒ String = { str ⇒
    str.takeWhile(_ != ':')
  }
}
