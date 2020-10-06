// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.scala.core.validations.ValidationError

object DomainValidationError {
  def makeErrorString(validationErrors: Seq[ValidationError]): String = {
    val allErrors = validationErrors.map(_.message).mkString("\n")
    s"Validation failed, errors:\n$allErrors"
  }
}

case class DomainValidationError(validationErrors: Seq[ValidationError]) extends Exception(DomainValidationError.makeErrorString(validationErrors))
