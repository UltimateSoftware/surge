// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.health.domain.Error

class ErrorSpec extends AnyWordSpec with Matchers {

  "Error" should {
    "create copy with error" in {
      val error = Error("error", None, None)

      val errorWithError = error.withError(new RuntimeException("added error"))

      errorWithError.error.isDefined shouldEqual true
      errorWithError.error.get shouldBe a[RuntimeException]
      errorWithError.equals(error) shouldEqual false
    }
  }
}
