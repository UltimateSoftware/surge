// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.javadsl

import surge.core.{ DomainValidationError ⇒ ScalaDomainValidationError }
import surge.scala.core.validations.ValidationError

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
