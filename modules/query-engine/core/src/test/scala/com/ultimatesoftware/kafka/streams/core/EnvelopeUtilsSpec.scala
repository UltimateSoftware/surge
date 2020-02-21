// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import org.scalatest.{ Matchers, WordSpec }

class EnvelopeUtilsSpec extends WordSpec with Matchers {
  private def mockFormatting(event: String, meta: Option[String]): SurgeEventReadFormatting[String, String] =
    (_: Array[Byte]) ⇒ event -> meta

  "EnvelopeUtils" should {
    "Return None if the formatting can't parse an event from bytes" in {
      val throwsExceptionMock = new SurgeEventReadFormatting[String, String] {
        override def readEvent(bytes: Array[Byte]): (String, Option[String]) = throw new RuntimeException("This is expected")
      }
      val utils = new EnvelopeUtils(throwsExceptionMock)
      utils.eventFromBytes(Array.emptyByteArray) shouldEqual None
    }

    "Return None if the formatting can't extract metadata from the bytes" in {
      val utils = new EnvelopeUtils(mockFormatting("event", None))
      utils.eventFromBytes(Array.emptyByteArray) shouldEqual None
    }

    "Return some event plus metadata if both the event and metadata can be parsed from the bytes" in {
      val utils = new EnvelopeUtils(mockFormatting("event2", Some("metadata")))
      utils.eventFromBytes(Array.emptyByteArray) shouldEqual Some(EventPlusMeta("event2", "metadata"))
    }
  }
}
