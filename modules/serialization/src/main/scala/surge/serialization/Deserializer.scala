// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.serialization

trait Deserializer[TYPE] {
  def deserialize(bytes: Array[Byte]): TYPE
}
