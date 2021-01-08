// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.validations

import java.util.UUID
import java.util.concurrent.TimeoutException

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ ExecutionContext, Future }

class ValidationTests extends AsyncWordSpec with Matchers {
  import ValidationDSL._

  private implicit val ec: ExecutionContext = ExecutionContext.global

  private val DEFAULT_ERROR_MESSAGE = "ERROR!!"
  private val DEFAULT_ERROR_RESPONSE = Left(Seq(ValidationError(DEFAULT_ERROR_MESSAGE)))

  "ValidateWith" should {

    "validate a valid value given a predicate" in {
      val s = "HELLO"
      val result = s mustBe ValidateWith[String](_.nonEmpty) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(s)
    }

    "invalidate with the default message provided by the library" in {
      val result = "" mustBe ValidateWith[String](_.nonEmpty) orElseDefaultMessage

      result shouldBe Left(Seq(ValidationError("String not valid")))
    }

    "invalidate a an invalid value given a predicate" in {
      val result = "" mustBe ValidateWith[String](_.nonEmpty) orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }

    "create a new validator given a predicate and use it to validate a valid value" in {
      val ValidGreeting = ValidateWith[String](s => Set("HELLO", "HI", "GOOD DAY").contains(s))
      val s = "HELLO"
      val result = s mustBe ValidGreeting orElse DEFAULT_ERROR_MESSAGE
      result shouldBe Right(s)
    }

    "create a new validator given a predicate and use it to invalidate an invalid value" in {
      val ValidGreeting = ValidateWith[String](s => Set("HELLO", "HI", "GOOD DAY").contains(s))
      val s = "GOODBYE"
      val result = s mustBe ValidGreeting orElse DEFAULT_ERROR_MESSAGE
      result shouldBe DEFAULT_ERROR_RESPONSE
    }
  }

  "AsyncValidateWith" should {

    val RESOURCE_UUID = UUID.randomUUID()

    // Mock latent call to com.ultimatesoftware.scala.core.resource service
    def verifyResourceExists: UUID => Future[Boolean] = _ => Future(true)
    def verifyResourceExistsInvalid: UUID => Future[Boolean] = _ => Future(false)
    def verifyResourceExistsTimeout: UUID => Future[Boolean] = _ => Future.failed(new TimeoutException("Timeout"))

    "validate a result based on an successful asynchronous predicate" in {
      val result = AsyncValidateWith[UUID](s => verifyResourceExists(s)) apply (RESOURCE_UUID, Some(DEFAULT_ERROR_MESSAGE))
      result map (_ shouldBe Right(RESOURCE_UUID))
    }

    "invalidate a result based on an failed asynchronous predicate" in {
      val result = AsyncValidateWith[UUID](s => verifyResourceExistsInvalid(s)) apply (RESOURCE_UUID, Some(DEFAULT_ERROR_MESSAGE))
      result map (_ shouldBe DEFAULT_ERROR_RESPONSE)
    }

    "invalidate a result based on a timeout of an asynchronous predicate" in {
      val result = AsyncValidateWith[UUID](s => verifyResourceExistsTimeout(s)) apply (RESOURCE_UUID, Some(DEFAULT_ERROR_MESSAGE))
      result map (_ shouldBe Left(Seq(ValidationError("Timeout"))))
    }

    "create a new validator given an asynchronous predicate and use it to validate a valid value" in {
      val ValidResourceId = AsyncValidateWith[UUID](id => verifyResourceExists(id))
      val result = RESOURCE_UUID mustBe ValidResourceId orElse DEFAULT_ERROR_MESSAGE
      result map (_ shouldBe Right(RESOURCE_UUID))
    }

    "create a new validator given an asynchronous predicate and use it to invalidate an invalid value" in {
      val ValidResourceId = AsyncValidateWith[UUID](id => verifyResourceExistsInvalid(id))
      val result = RESOURCE_UUID mustBe ValidResourceId orElse DEFAULT_ERROR_MESSAGE
      result map (_ shouldBe DEFAULT_ERROR_RESPONSE)
    }
  }
}
