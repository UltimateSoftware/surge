// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class SerializedMessageSpec extends AnyWordSpec {

  "Create Serialized Message" in {
    val message = SerializedMessage.create("key", "value".getBytes())

    message.key shouldEqual "key"
    message.value shouldEqual "value".getBytes()
  }

  "Create Serialized Message with Headers" in {
    val message = SerializedMessage.create(key = "key", value = "value".getBytes(), headers = Map[String, String]("key" -> "value").asJava)

    message.key shouldEqual "key"
    message.value shouldEqual "value".getBytes()

    message.headers.get("key") shouldEqual Some("value")
  }
}
