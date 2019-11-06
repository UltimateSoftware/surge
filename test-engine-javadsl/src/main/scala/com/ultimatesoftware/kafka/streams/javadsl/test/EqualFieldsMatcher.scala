// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl.test

import java.lang.reflect.Field
import java.util.Objects

import scala.util.{ Failure, Success, Try }

case class FailedField(field: Field, expectedValue: AnyRef, actualValue: AnyRef)

class EqualFieldsMatcher[T](val expected: T) {
  def matches(item: Any): Boolean = expected.getClass.isInstance(item) && matchesSafely(item)

  private def matchesSafely(actual: Any): Boolean = expected.getClass == actual.getClass && this.fieldsMatch(expected.getClass, expected, actual)

  private def fieldsMatch(aClass: Class[_], expectedValue: Any, actual: Any): Boolean = {
    val declaredFields = aClass.getDeclaredFields
    val mismatchedFields = for {
      index ← 0 until declaredFields.length
    } yield {
      val field = declaredFields(index)
      field.setAccessible(true)
      Try {
        val expectedFieldValue = field.get(expectedValue)
        val actualFieldValue = field.get(actual)
        if (!Objects.deepEquals(expectedFieldValue, actualFieldValue)) {
          Some(FailedField(field, expectedFieldValue, actualFieldValue))
        } else {
          None
        }
      } match {
        case Success(value) ⇒ value
        case Failure(_) ⇒
          throw new SurgeAssertionError("Could not confirm object equality due to an exception")
      }
    }

    val isMatch = mismatchedFields.flatten.isEmpty
    if (aClass.getSuperclass ne classOf[Any]) {
      isMatch && fieldsMatch(aClass.getSuperclass, expectedValue, actual)
    } else {
      isMatch
    }
  }
}
