// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.health.domain.Warning

class WarningSpec extends AnyWordSpec with Matchers {

  "Warning" should {
    "create copy with error" in {
      val warning = Warning("warning", None, None)

      val warningWithError = warning.withError(new RuntimeException("added error"))

      warningWithError.error.isDefined shouldEqual true
      warningWithError.error.get shouldBe a[RuntimeException]
      warningWithError.equals(warning) shouldEqual false
    }
  }
}
