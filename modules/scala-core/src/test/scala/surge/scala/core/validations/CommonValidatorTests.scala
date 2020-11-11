// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.validations

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CommonValidatorTests extends AnyWordSpec with Matchers {
  import FieldValidators._
  import ValidationDSL._

  private val DEFAULT_ERROR_MESSAGE = "ERROR!!"
  private val DEFAULT_ERROR_RESPONSE = Left(Seq(ValidationError(DEFAULT_ERROR_MESSAGE)))

  "SatisfyOneOf" should {

    "validate a value against any one of group of validators" in {
      val str = "111-33-5555"
      val result = str mustBe SatisfyAtLeastOneOf[String](ValidUsSSN, ValidCanadianSIN) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(str)
    }

    "validate another value against any one of group of validators" in {
      val str = "111-333-555"
      val result = str mustBe SatisfyAtLeastOneOf[String](ValidUsSSN, ValidCanadianSIN) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(str)
    }

    "invalidate a value that does not satisfy any of a group validators" in {
      val str = "111-33-55"
      val result = str mustBe SatisfyAtLeastOneOf[String](ValidUsSSN, ValidCanadianSIN) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "HoldSomeValue" should {

    "validate Some value" in {
      val opt: Option[Int] = Some[Int](1)
      val result = opt must HoldSomeValue[Int] orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(Some(1))
    }

    "invalidate a None" in {
      val opt: Option[Int] = None
      val result = opt must HoldSomeValue[Int] orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "NonEmptyString" should {

    "validate a good String" in {
      val result = "Hello" mustBe NonEmptyString orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right("Hello")
    }

    "invalidate a blank String" in {
      val result = "" mustBe NonEmptyString orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "HoldSomeNonEmptyString" should {

    "validate Some good string" in {
      val opt: Option[String] = Some("Hello")
      val result = opt must HoldSomeNonEmptyString orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(Some("Hello"))
    }

    "invalidate a Some empty string" in {
      val opt: Option[String] = Some("")
      val result = opt must HoldSomeNonEmptyString orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
    "invalidate a None" in {
      val opt: Option[String] = None
      val result = opt must HoldSomeNonEmptyString orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "PositiveNumber" should {

    "validate a positive number" in {
      val result = 1 mustBe PositiveNumber[Int] orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(1)
    }

    "invalidate a non positive number" in {
      val result = -1 mustBe PositiveNumber[Int] orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "NonNegativeNumber" should {

    "validate a non negative number" in {
      val result = 1 mustBe PositiveNumber[Int] orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(1)
    }

    "invalidate a negative number" in {
      val result = -1 mustBe PositiveNumber[Int] orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "InTheRange" should {

    "validate a number in the range" in {
      val result = 20 mustBe InTheRange(10, 30) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(20)
    }

    "invalidate a number below the range" in {
      val result = 5 mustBe InTheRange(10, 30) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }

    "invalidate a number above the range" in {
      val result = 35 mustBe InTheRange(10, 30) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "ContainAtLeastOneItem" should {

    "validate a non empty Collection" in {
      val col = Seq(1d, 3d, 4d)
      val result = col must ContainAtLeastOneItem[Double] orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(col)
    }

    "invalidate an empty Collection" in {
      val result = Seq.empty[Int] must ContainAtLeastOneItem[Int] orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "ContainOnlyOneOfEach" should {

    "validate a unique Collection" in {
      val col = Seq(1, 2, 3)
      val result = col must ContainOnlyOneOfEach[Int, Int](identity) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(col)
    }

    "invalidate a non unique Collection" in {
      val result = Seq(1, 1, 3) must ContainOnlyOneOfEach[Int, Int](identity) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "SatisfyForAllItems" should {

    "validate all items in a Collection" in {
      val col = Seq(1, 2, 3)
      val result = col must SatisfyForAllItems[Int](_ < 5) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(col)
    }

    "invalidate a collection with any invalid items" in {
      val col = Seq(1, 2, 8)
      val result = col must SatisfyForAllItems[Int](_ < 5) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "ValidateForAllItems" should {

    "validate all items in a Collection" in {
      val col = Seq(1, 2, 3)
      val result = col must ValidateForAllItems[Int](PositiveNumber) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(col)
    }

    "invalidate a collection with any invalid items" in {
      val col = Seq(1, 2, -1)
      val result = col must ValidateForAllItems[Int](PositiveNumber) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "ValidEmailAddress" should {

    "validate a good email address" in {
      val email = "ultitim@ultimatesoftware.com"
      val result = email mustBe ValidEmailAddress orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(email)
    }

    "invalidate a bad email address" in {
      val email = "ultitim at ultimate dot com"
      val result = email mustBe ValidEmailAddress orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "ValidURL" should {

    "validate a good url" in {
      val email = "http://www.ultimatesoftware.com"
      val result = email mustBe ValidURL orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(email)
    }

    "invalidate a bad url" in {
      val email = "ultimate dot com"
      val result = email mustBe ValidURL orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "HaveMaxDigits" should {
    "validate a good integer" in {
      val number = 1000
      val result = number must HaveMaxDigits[Int](integer = 5) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(number)
    }

    "validate a good double" in {
      val number = 1000.0123
      val result = number must HaveMaxDigits[Double](integer = 5, fraction = 4) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(number)
    }

    "invalidate a bad integer" in {
      val number = 1000
      val result = number must HaveMaxDigits[Int](integer = 3) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }

    "invalidate a bad double" in {
      val invalidIntSide = 100.01
      val invalidIntSideResult = invalidIntSide must HaveMaxDigits[Double](integer = 2, fraction = 2) orElse DEFAULT_ERROR_MESSAGE
      invalidIntSideResult shouldEqual DEFAULT_ERROR_RESPONSE

      val invalidFractionSide = 10.001
      val invalidFractionSideResult = invalidFractionSide must HaveMaxDigits[Double](integer = 2, fraction = 2) orElse DEFAULT_ERROR_MESSAGE
      invalidFractionSideResult shouldEqual DEFAULT_ERROR_RESPONSE
    }
  }

  "ValidAlphanumericString" should {
    "validate a good alphanumeric string" in {
      val string = "ULTImateSoftware1234"
      val result = string mustBe ValidAlphanumericString orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(string)
    }

    "invalidate a non-alphanumeric string" in {
      val spaces = "ultimate software"
      val spacesResult = spaces mustBe ValidAlphanumericString orElse DEFAULT_ERROR_MESSAGE
      spacesResult shouldBe DEFAULT_ERROR_RESPONSE

      val dashes = "ultimate-software"
      val dashesResult = dashes mustBe ValidAlphanumericString orElse DEFAULT_ERROR_MESSAGE
      dashesResult shouldBe DEFAULT_ERROR_RESPONSE
    }
  }
}
