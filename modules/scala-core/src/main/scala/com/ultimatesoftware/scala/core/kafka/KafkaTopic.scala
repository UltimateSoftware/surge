// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.kafka

import java.util.Optional

import scala.compat.java8.OptionConverters._

object KafkaTopic {

  def of(name: String): KafkaTopic = {
    of(name, compacted = false, Optional.empty())
  }

  def of(name: String, compacted: Boolean): KafkaTopic = {
    of(name, compacted, Optional.empty())
  }

  def of(name: String, compacted: Boolean = false, numberPartitionsOverride: Optional[Integer] = Optional.empty()): KafkaTopic = {
    val numPartitions = numberPartitionsOverride.asScala.map(_.intValue)
    KafkaTopic(name, compacted, numPartitions)
  }
}

final case class KafkaTopic(name: String, compacted: Boolean = false,
    numberPartitionsOverride: Option[Int] = None)
