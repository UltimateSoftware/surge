// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.validations

trait CollectionValidators {
  def ContainAtLeastOneItem[A]: Validator[Seq[A]] =
    ValidateWith[Seq[A]](_.nonEmpty, Some("The collection must contain at least one item"))

  def ContainOnlyOneOfEach[A, B](key: A ⇒ B): Validator[Seq[A]] =
    ValidateWith[Seq[A]](col ⇒ col.map(key).toSet.size == col.size, Some("The collection must not contain duplicates"))

  def SatisfyForAllItems[A](predicate: A ⇒ Boolean): Validator[Seq[A]] =
    ValidateWith[Seq[A]](col ⇒ col.forall(x ⇒ predicate(x)), Some("The collection's items must all satisfy the predicate"))

  def ValidateForAllItems[A](validator: Validator[A]): Validator[Seq[A]] =
    ValidateWith[Seq[A]](col ⇒ col.forall(x ⇒ validator(x, None).isRight), Some("The collection's items must all satisfy the validation"))
}
