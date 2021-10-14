// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import scala.jdk.CollectionConverters._

case class SerializedMessage(key: String, value: Array[Byte], headers: Map[String, String] = Map.empty)

object SerializedMessage {
  def create(key: String, value: Array[Byte], headers: java.util.Map[String, String]): SerializedMessage = {
    SerializedMessage(key, value, headers.asScala.toMap)
  }
  def create(key: String, value: Array[Byte]): SerializedMessage = {
    SerializedMessage(key, value)
  }
}