// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.windows

import java.time.Instant

import org.slf4j.{ Logger, LoggerFactory }
import surge.health.domain.HealthSignal

import scala.concurrent.duration.FiniteDuration

object Window {
  val log: Logger = LoggerFactory.getLogger(getClass)
  def windowFor(ts: Instant, duration: FiniteDuration): Window = {
    Window(ts.toEpochMilli, ts.plusMillis(duration.toMillis).toEpochMilli, duration = duration, data = Seq.empty)
  }
}

case class Window(from: Long, to: Long, data: Seq[HealthSignal], duration: FiniteDuration) {
  override def toString: String = s"At ${Instant.now().toEpochMilli} Window From $from To $to} - expired == ${expired()} - durationInMillis == $duration"

  def expired(): Boolean = {
    to <= Instant.now().toEpochMilli
  }
}
