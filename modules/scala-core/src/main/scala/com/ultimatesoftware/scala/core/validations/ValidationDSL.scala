// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.validations

/**
 * Validation DSL (Domain Specific Language) includes implicits that allow for more expressive validations.
 *
 * someValue mustBe {SomeValidator} orElse {Some Error Message String}
 *
 * For example,
 *
 * val firstName: String = ???
 * val result = firstName mustBe NonEmptyString orElse "The first name can not be blank"
 *
 * val employee: Employee = ???
 * val result = employee mustSatisfy EmployeeValidator
 *
 */
object ValidationDSL {

  implicit class Validatable[A](val value: A) {
    def must(validator: Validator[A]): IntermediateValidation[A] = IntermediateValidation(validator, this)
    def mustBe(validator: Validator[A]): IntermediateValidation[A] = IntermediateValidation(validator, this)
    def mustHave(validator: Validator[A]): IntermediateValidation[A] = IntermediateValidation(validator, this)
    def mustSatisfy(validator: Validator[A]): ValidationResult[A] = validator(value)

    def must(validator: AsyncValidator[A]): IntermediateAsyncValidation[A] = IntermediateAsyncValidation(validator, this)
    def mustBe(validator: AsyncValidator[A]): IntermediateAsyncValidation[A] = IntermediateAsyncValidation(validator, this)
    def mustHave(validator: AsyncValidator[A]): IntermediateAsyncValidation[A] = IntermediateAsyncValidation(validator, this)
    def mustSatisfy(validator: AsyncValidator[A]): AsyncValidationResult[A] = validator(value)
  }

  final case class IntermediateValidation[A](validator: Validator[A], validatable: Validatable[A]) {
    private def invoke: ValidationResult[A] = validator(validatable.value)

    def and[B](otherIntermediateValidations: IntermediateValidation[B]): IntermediateValidation[A] = {
      (invoke, otherIntermediateValidations.invoke) match {
        case (Right(value), Right(_)) ⇒ IntermediateValidation(_ ⇒ Right(value), validatable.value)
        case (Right(_), Left(lvm))    ⇒ IntermediateValidation(_ ⇒ Left(lvm), validatable.value)
        case (Left(lvm), Right(_))    ⇒ IntermediateValidation(_ ⇒ Left(lvm), validatable.value)
        case (Left(lvm), Left(olvm))  ⇒ IntermediateValidation(_ ⇒ Left(lvm ++ olvm), validatable.value)
      }
    }

    def orElse(errorMessage: String): ValidationResult[A] = validator((validatable.value, Some(errorMessage)))
    def orElseDefaultMessage: ValidationResult[A] = invoke
  }

  final case class IntermediateAsyncValidation[A](validator: AsyncValidator[A], validatable: Validatable[A]) {
    def orElse(errorMessage: String): AsyncValidationResult[A] = validator((validatable.value, Some(errorMessage)))
    def orElseDefaultMessage: AsyncValidationResult[A] = validator(validatable.value)
  }

  implicit def validationResultToAsync[A](validationResult: ValidationResult[A]): AsyncValidationResult[A] = validationResult.toAsync

  implicit def valueToValidationInputWithDefaultMessage[A](value: A): ValidatorInput[A] = (value, None)

  implicit def objectValidatorToAsync[A](objectValidator: ObjectValidator[A]): AsyncObjectValidator[A] = objectValidator.toAsync
}
