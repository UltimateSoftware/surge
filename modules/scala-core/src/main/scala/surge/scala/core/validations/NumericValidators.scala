// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.validations

trait NumericValidators {
  def PositiveNumber[A: Numeric](implicit numeric: Numeric[A]): Validator[A] =
    ValidateWith[A](num => numeric.compare(numeric.zero, num) < 0, Some("The number must be positive"))

  def NonNegativeNumber[A: Numeric](implicit numeric: Numeric[A]): Validator[A] =
    ValidateWith[A](num => numeric.compare(numeric.zero, num) <= 0, Some("The number must be non negative"))

  def InTheRange[A](low: A, high: A)(implicit ord: Ordering[A]): Validator[A] =
    ValidateWith[A](num => ord.compare(low, num) < 0 & ord.compare(num, high) < 0, Some("The number must be in the range"))

  def HaveMaxDigits[A: Numeric](integer: Int, fraction: Int = 0): Validator[A] = ValidateWith[A] { num =>
    val decimalRegex = "([0-9]*)\\.?([0-9]*)".r
    num.toString match {
      case decimalRegex(beforeDecimal, afterDecimal) =>
        beforeDecimal.length <= integer && afterDecimal.length <= fraction
    }
  }
}
