// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.core

import scala.jdk.CollectionConverters._

case class SerializedAggregate(value: Array[Byte], headers: Map[String, String] = Map.empty)

object SerializedAggregate {
  def create(value: Array[Byte], headers: java.util.Map[String, String]): SerializedAggregate = {
    SerializedAggregate(value, headers.asScala.toMap)
  }
  def create(value: Array[Byte]): SerializedAggregate = {
    SerializedAggregate(value)
  }
}
