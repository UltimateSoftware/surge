// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.validations

import java.util.UUID

import scala.util.matching.Regex
import scala.util.{ Failure, Success, Try }

trait StringValidators {
  def MatchRegex(regex: Regex, optErrMsg: Option[ValidationMessage] = None): Validator[String] =
    ValidateWith[String](str ⇒ regex.findFirstMatchIn(str).isDefined, optErrMsg orElse Some("The string must match the regular expression"))

  def MatchRegexString(regexString: String, optErrMsg: Option[ValidationMessage] = None): Validator[String] =
    ValidateWith[String](str ⇒ regexString.r.findFirstMatchIn(str).isDefined, optErrMsg orElse Some("The string must match the regular expression"))

  val NonEmptyString: Validator[String] =
    ValidateWith[String](_.nonEmpty, Some("The string must be non empty"))

  val HoldSomeNonEmptyString: Validator[Option[String]] =
    ValidateWith[Option[String]](_.getOrElse("").length > 0, Some("The option must hold a non empty string"))

  val ValidUUID: Validator[String] =
    ValidateWith[String]({ str ⇒
      Try(UUID.fromString(str)) match {
        case Success(_) ⇒ true
        case Failure(_) ⇒ false
      }
    }, Some("The string must be a valid UUID"))

  private val emailRegex = "^[a-zA-Z0-9\\.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$".r
  private val urlRegex = "^(\\w+):\\/{2}(\\w*)\\.?([^\\/]*)([^\\?]*)\\??(.*)?".r
  private val usSsnRegex = "^(\\d{3}-?\\d{2}-?\\d{4}|XXX-XX-XXXX)$".r
  private val usEinRegex = "^(\\d{2}-?\\d{7}|XX-XXXXXXX)$".r
  private val usPhoneNumberRegex = "^(\\d{3}-?\\d{3}-?\\d{4}|XXX-XXX-XXXX)$".r
  private val canadianSinRegex = "^(\\d{3}-?\\d{3}-?\\d{3}|XXX-XXX-XXX)$".r
  private val alphanumericStringRegex = "^[a-zA-Z0-9]*$".r

  val ValidEmailAddress: Validator[String] = MatchRegex(emailRegex, Some("The string must be an valid email address"))
  val ValidURL: Validator[String] = MatchRegex(urlRegex, Some("The string must be a valid URL"))
  val ValidUsSSN: Validator[String] = MatchRegex(usSsnRegex, Some("The string must be a valid US SSN"))
  val ValidUsEIN: Validator[String] = MatchRegex(usEinRegex, Some("The string must be a valid US EIN"))
  val ValidUsPhoneNumber: Validator[String] = MatchRegex(usPhoneNumberRegex, Some("The string must be a valid US Phone Number"))
  val ValidCanadianSIN: Validator[String] = MatchRegex(canadianSinRegex, Some("The string must be a valid Canadian SIN"))
  val ValidAlphanumericString: Validator[String] = MatchRegex(alphanumericStringRegex, Some("The string must be alphanumeric"))
}
