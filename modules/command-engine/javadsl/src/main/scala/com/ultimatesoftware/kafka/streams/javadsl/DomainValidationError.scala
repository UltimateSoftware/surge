// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import com.ultimatesoftware.kafka.streams.core.{ DomainValidationError ⇒ ScalaDomainValidationError }
import com.ultimatesoftware.scala.core.validations.ValidationError

import scala.collection.JavaConverters._

object DomainValidationError {
  implicit class ScalaDomainValidationErrorExtensions(scalaValidationError: ScalaDomainValidationError) {
    def asJava: DomainValidationError = {
      DomainValidationError(scalaValidationError.validationErrors.asJava)
    }
  }
}

case class DomainValidationError(validationErrors: java.util.List[ValidationError])
  extends Exception(ScalaDomainValidationError.makeErrorString(validationErrors.asScala))
