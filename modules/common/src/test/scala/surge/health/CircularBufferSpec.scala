// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CircularBufferSpec extends AnyWordSpec with Matchers {

  "CircularBuffer" should {
    "append item" in {
      val buffer = new CircularBuffer[String](10)
      buffer.push("Hello")

      buffer.getAll shouldEqual Array("Hello")
    }

    "clear" in {
      val buffer = new CircularBuffer[String](10)
      buffer.push("Hello")

      buffer.clear()

      buffer.getAll shouldEqual Array()
    }

    "round robin" in {
      val buffer = new CircularBuffer[String](size = 3)

      buffer.push("one")
      buffer.push("two")
      buffer.push("three")
      buffer.push("four")

      buffer.getAll shouldEqual Array("four", "two", "three")
    }
  }
}
