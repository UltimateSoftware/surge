// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core

import java.util.{ Base64, UUID }

package object utils {
  val EmptyUUIDString = "00000000-0000-0000-0000-000000000000"
  val EmptyUUID: UUID = UUID.fromString(EmptyUUIDString)

  object BinaryEncoding {
    @inline def encode(bytes: Array[Byte]): String = Base64.getEncoder.encodeToString(bytes)
    @inline def decode(str: String): Array[Byte] = Base64.getDecoder.decode(str)
  }
}
