// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.serialization

case class BytesPlusHeaders(bytes: Array[Byte], headers: Map[String, String] = Map.empty)

trait Serializer[TYPE] {
  def serialize(value: TYPE): BytesPlusHeaders
}