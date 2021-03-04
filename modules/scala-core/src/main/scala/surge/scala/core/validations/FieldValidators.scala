// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.validations

trait FieldValidators extends StringValidators with CollectionValidators with NumericValidators {

  def EqualTo[A](other: A): Validator[A] =
    ValidateWith[A](_ == other, Some(s"The value must equal $other"))

  def NotEqualTo[A](other: A): Validator[A] =
    ValidateWith[A](_ != other, Some(s"The value must not equal $other"))

  def LessThan[A](other: A)(implicit ord: Ordering[A]): Validator[A] =
    ValidateWith[A](value => ord.compare(value, other) < 0, Some(s"The value must be less than $other"))

  def LessThanOrEqualTo[A](other: A)(implicit ord: Ordering[A]): Validator[A] =
    ValidateWith[A](value => ord.compare(value, other) <= 0, Some(s"The value must be less than or equal to $other"))

  def GreaterThan[A](other: A)(implicit ord: Ordering[A]): Validator[A] =
    ValidateWith[A](value => ord.compare(other, value) < 0, Some(s"The value must be greater than $other"))

  def GreaterThanOrEqualTo[A](other: A)(implicit ord: Ordering[A]): Validator[A] =
    ValidateWith[A](value => ord.compare(other, value) <= 0, Some(s"The value must be greater than or equal to $other"))

  def NotSatisfy[A](validator: Validator[A]): Validator[A] = {
    case input @ (value, optErrMsg) =>
      validator(input) match {
        case Right(_) => Left(Seq(ValidationError(optErrMsg getOrElse DEFAULT_VALIDATION_MESSAGE)))
        case Left(_)  => Right(value)
      }
  }

  def SatisfyAtLeastOneOf[A](validators: Validator[A]*): Validator[A] = ValidateWith[A](
    value => validators.toSeq.exists(validator => validator((value, None)).isRight),
    Some("The value must satisfy at least one of the validators"))

  def SatisfyAllOf[A](validators: Validator[A]*): Validator[A] = ValidateWith[A](
    value => !validators.toSeq.exists(validator => validator((value, None)).isLeft),
    Some("The value must satisfy all of the validators"))

  def HoldSomeValue[A]: Validator[Option[A]] =
    ValidateWith(_.isDefined, Some("The option must hold a value"))
}

object FieldValidators extends FieldValidators
