// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.validations

object CommandValidator {
  def apply[A, B](validators: FieldValidator[MessagePlusCurrentAggregate[A, B]]): ObjectValidator[MessagePlusCurrentAggregate[A, B]] =
    ObjectValidator[MessagePlusCurrentAggregate[A, B]] { validators }
}

object AsyncCommandValidator {
  def apply[A, B](validators: AsyncFieldValidator[MessagePlusCurrentAggregate[A, B]]): AsyncObjectValidator[MessagePlusCurrentAggregate[A, B]] =
    AsyncObjectValidator[MessagePlusCurrentAggregate[A, B]] { validators }
}
