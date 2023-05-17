// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class SerializedAggregateSpec extends AnyWordSpec {

  "Create Serialized Aggregate" in {
    val message = SerializedAggregate.create("value".getBytes())

    message.value shouldEqual "value".getBytes()
  }

  "Create Serialized Aggregate with Headers" in {
    val message = SerializedAggregate.create(value = "value".getBytes(), headers = Map[String, String]("key" -> "value").asJava)

    message.value shouldEqual "value".getBytes()

    message.headers.get("key") shouldEqual Some("value")
  }
}
