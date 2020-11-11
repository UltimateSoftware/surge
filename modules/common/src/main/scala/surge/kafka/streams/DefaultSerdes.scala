// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import org.apache.kafka.common.serialization.{ Serde, Serdes }

/**
 * Default serializers/deserializers for simple base types
 */
object DefaultSerdes {
  implicit val stringSerde: Serde[String] = Serdes.String()
  implicit val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]
  implicit val byteArraySerde: Serde[Array[Byte]] = Serdes.ByteArray()
  implicit val bytesSerde: Serde[org.apache.kafka.common.utils.Bytes] = Serdes.Bytes()
  implicit val floatSerde: Serde[Float] = Serdes.Float().asInstanceOf[Serde[Float]]
  implicit val doubleSerde: Serde[Double] = Serdes.Double().asInstanceOf[Serde[Double]]
  implicit val integerSerde: Serde[Int] = Serdes.Integer().asInstanceOf[Serde[Int]]
}
