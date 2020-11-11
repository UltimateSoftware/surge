// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.utils

import java.time.Instant

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TimeEnhancementsSpec extends AnyWordSpec with Matchers {
  import TimeEnhancements._

  private val secondsPerMinute = 60
  private val secondsPerHour = 60 * secondsPerMinute
  private val secondsPerDay = 24 * secondsPerHour

  "Instant with TimeEnhancements" should {
    "Be able to correctly add minutes" in {
      val now = Instant.now

      val twoMinutesAgo = now.plusMinutes(-2)
      twoMinutesAgo shouldEqual now.plusSeconds(-2 * secondsPerMinute)

      val stillNow = now.plusMinutes(0)
      stillNow shouldEqual now

      val fiveMinutesFromNow = now.plusMinutes(5)
      fiveMinutesFromNow shouldEqual now.plusSeconds(5 * secondsPerMinute)
    }

    "Be able to correctly add hours" in {
      val now = Instant.now

      val twoHoursAgo = now.plusHours(-2)
      twoHoursAgo shouldEqual now.plusSeconds(-2 * secondsPerHour)

      val stillNow = now.plusHours(0)
      stillNow shouldEqual now

      val fiveHoursFromNow = now.plusHours(5)
      fiveHoursFromNow shouldEqual now.plusSeconds(5 * secondsPerHour)
    }

    "Be able to correctly add days" in {
      val now = Instant.now

      val twoDaysAgo = now.plusDays(-2)
      twoDaysAgo shouldEqual now.plusSeconds(-2 * secondsPerDay)

      val stillNow = now.plusDays(0)
      stillNow shouldEqual now

      val fiveDaysFromNow = now.plusDays(5)
      fiveDaysFromNow shouldEqual now.plusSeconds(5 * secondsPerDay)
    }
  }
}
