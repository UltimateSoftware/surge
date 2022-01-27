// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.exceptions

case class SurgeInitializationException(message: String, cause: Throwable) extends RuntimeException(message, cause)
