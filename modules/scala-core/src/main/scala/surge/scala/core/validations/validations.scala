// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions

// TODO at some point we may want to leverage this https://ultigit.ultimatesoftware.com/projects/NUPL/repos/problem/browse
//  to more easily conform to the problem+json part of the standards https://docs.ulti.io/guardrails/standards/rest.html#13-error-handling
//  these validations provide a great declarative way to validate models, which the above library does not provide - a lot of the error builder
//  code I've read through passes an error builder around to a bunch of places and it's not easy to read through and validate domain logic. That
//  library does, however, provide an easy way to conform to the error reporting standards.
package object validations {

  // Validations Domain Model

  val DEFAULT_VALIDATION_MESSAGE: String = "The value is not valid"

  type ValidationMessage = String
  type ValidatorInput[A] = (A, Option[ValidationMessage])
  final case class ValidationError(message: ValidationMessage)

  type ValidationPredicate[A] = A ⇒ Boolean
  type Validator[A] = ValidatorInput[A] ⇒ ValidationResult[A]
  type ValidationResult[A] = Either[Seq[ValidationError], A]
  type FieldValidator[A] = A ⇒ Seq[ValidationResult[_]]

  type AsyncValidationPredicate[A] = A ⇒ Future[Boolean]
  type AsyncValidator[A] = ValidatorInput[A] ⇒ AsyncValidationResult[A]
  type AsyncValidationResult[A] = Future[ValidationResult[A]]
  type AsyncFieldValidator[A] = A ⇒ Seq[AsyncValidationResult[_]]

  implicit class ValidationResultExtensions[A](validationResult: ValidationResult[A]) {
    def toAsync: AsyncValidationResult[A] = Future.successful(validationResult)
  }

  implicit class FieldValidatorExtensions[A](fieldValidator: FieldValidator[A]) {
    def toAsync: AsyncFieldValidator[A] = fieldValidator.andThen(_.map(_.toAsync))
  }

  /**
   * ValidateWith is a basic synchronous validator that can be used directly as so:
   *
   * val validationResult = ValidateWith[String](s ⇒ s.length > 0)("String to Test", Some("String can not be empty"))
   *
   * or use it as a factory to create other validations as so:
   *
   * val NonEmptyString: Validator[String] = ValidateWith(s => s.length > 0)
   *
   * then use the new validator more conveniently:
   *
   * val validationResult = NonEmptyString("String to Test", Some("String can not be empty"))
   *
   * @param predicate a function represents the validation
   * @tparam A the type being validated
   * @return A ValidationResult, either a Right(A) or a Left(Seq[ValidationError])
   */

  def ValidateWith[A](predicate: ValidationPredicate[A], optErrMsg: Option[ValidationMessage] = None): Validator[A] = {
    case (value, _) if predicate(value) ⇒ Right(value)
    case (value, oem)                   ⇒ Left(Seq(ValidationError(oem orElse optErrMsg getOrElse s"${value.getClass.getSimpleName} not valid")))
  }

  /**
   * ValidateWith is a basic synchronous validator that can be used directly as so:
   *
   * val asyncValidationResult = AsyncValidateWith[String](s ⇒ Future(s.length > 0))("String to Test", Some("String can not be empty"))
   *
   * or use it as a factory to create other validations as so:
   *
   * val AsyncNonEmptyString: AsyncValidator[String] = AsyncValidateWith(s => Future(s.length > 0))
   *
   * then use the new validator more conveniently:
   *
   * val asyncValidationResult = AsyncNonEmptyString("String to Test", Some("String can not be empty"))
   *
   * @param predicate a function represents the validation
   * @tparam A the type being validated
   * @return A ValidationResult, either a Right(A) or a Left(Seq[ValidationError])
   */
  def AsyncValidateWith[A](predicate: AsyncValidationPredicate[A], optErrMsg: Option[ValidationMessage] = None)(implicit ec: ExecutionContext): AsyncValidator[A] = {
    case (value, oem) ⇒
      predicate(value).recover({ case ex ⇒ (false, ex) }).map {
        case true                   ⇒ Right(value)
        case false                  ⇒ Left(Seq(ValidationError(oem orElse optErrMsg getOrElse DEFAULT_VALIDATION_MESSAGE)))
        case (false, ex: Throwable) ⇒ Left(Seq(ValidationError(ex.getMessage)))
      }
  }

  type CommandValidator[A, B] = ObjectValidator[MessagePlusCurrentAggregate[A, B]]
  type AsyncCommandValidator[A, B] = AsyncObjectValidator[MessagePlusCurrentAggregate[A, B]]

  implicit class ObjectValidatorExtensions[A, B](objectValidator: ObjectValidator[A]) {
    def toAsync: AsyncObjectValidator[A] = AsyncObjectValidator(objectValidator.fieldValidator.toAsync)
  }
  // END Validations Domain Model

  // Common Field level Validators - may be moved to a separate file
}
