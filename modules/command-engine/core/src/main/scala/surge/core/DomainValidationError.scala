// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import surge.scala.core.validations.ValidationError

object DomainValidationError {
  def makeErrorString(validationErrors: Seq[ValidationError]): String = {
    val allErrors = validationErrors.map(_.message).mkString("\n")
    s"Validation failed, errors:\n$allErrors"
  }
}

case class DomainValidationError(validationErrors: Seq[ValidationError]) extends Exception(DomainValidationError.makeErrorString(validationErrors))
