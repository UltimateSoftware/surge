// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.exceptions

case class SurgeUnexpectedException(underlying: Throwable) extends Throwable {
  override def toString: String =
    s"Surge unexpected exception, please report it to the Surge team ${underlying}"
}
