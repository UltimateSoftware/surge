// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.serialization

import surge.core.{ SerializedAggregate, SerializedMessage, SerializedWrapperSupport }

import scala.jdk.CollectionConverters._

object BytesPlusHeaders {
  def create(bytes: Array[Byte], headers: java.util.Map[String, String]): BytesPlusHeaders = {
    BytesPlusHeaders(bytes, headers.asScala.toMap)
  }
}

case class BytesPlusHeaders(bytes: Array[Byte], headers: Map[String, String] = Map.empty) extends SerializedWrapperSupport {
  override def asSerializedMessage(key: String): SerializedMessage = {
    SerializedMessage(key, bytes, headers)
  }

  override def asSerializedAggregate(): SerializedAggregate = {
    SerializedAggregate(bytes, headers)
  }
}

trait Serializer[TYPE] {
  def serialize(value: TYPE): BytesPlusHeaders
}
