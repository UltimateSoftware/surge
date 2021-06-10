// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.domain

import org.slf4j.{ Logger, LoggerFactory }

object EmittableHealthSignal {
  val logger: Logger = LoggerFactory.getLogger(getClass)
}

trait EmittableHealthSignal {
  def emit(): EmittableHealthSignal
  def logAsWarning(error: Option[Throwable] = None): EmittableHealthSignal
  def logAsDebug(): EmittableHealthSignal
  def logAsTrace(): EmittableHealthSignal
  def logAsError(error: Option[Throwable] = None): EmittableHealthSignal

  def handled(wasHandled: Boolean): EmittableHealthSignal

  def underlyingSignal(): HealthSignal
}
