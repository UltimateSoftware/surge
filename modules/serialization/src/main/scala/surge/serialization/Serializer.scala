// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.serialization

trait Serializer[TYPE] {
  def serialize(value: TYPE): Array[Byte]
}
