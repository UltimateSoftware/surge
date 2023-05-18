// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.exceptions

case class SurgeUnexpectedException(underlying: Throwable) extends Throwable {
  override def toString: String =
    s"Surge unexpected exception, please report it to the Surge team $underlying"
}
