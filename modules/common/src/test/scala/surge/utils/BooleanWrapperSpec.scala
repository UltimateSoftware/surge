// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BooleanWrapperSpec extends AnyWordSpec with Matchers {

  "BooleanWrapper" should {
    "initially be false" in {
      new BooleanWrapper().wrapped() shouldEqual false
    }

    "be true when copying asTrue" in {
      val isTrue = new BooleanWrapper().asTrue()

      isTrue.wrapped() shouldEqual true
    }

    "be false when copying asFalse" in {
      val isFalse = new BooleanWrapper().asFalse()
      isFalse.wrapped() shouldEqual false
    }
  }
}
