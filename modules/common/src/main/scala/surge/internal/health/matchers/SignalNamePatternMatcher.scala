// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.matchers

import java.util.regex.Pattern

import surge.health.domain.HealthSignal
import surge.health.matchers.{ SideEffect, SignalPatternMatchResult, SignalPatternMatcher }

import scala.concurrent.duration.FiniteDuration

object SignalNamePatternMatcher {
  def endsWith(suffix: String): SignalNamePatternMatcher = {
    SignalNamePatternMatcher(Pattern.compile(suffix + "$"))
  }

  def beginsWith(prefix: String): SignalNamePatternMatcher = {
    SignalNamePatternMatcher(Pattern.compile("^" + prefix))
  }
}

case class SignalNamePatternMatcher(pattern: Pattern, sideEffect: Option[SideEffect] = None) extends SignalPatternMatcher {
  override def searchForMatch(signals: Seq[HealthSignal], frequency: FiniteDuration): SignalPatternMatchResult = {
    val matches = signals.filter(signal => pattern.matcher(signal.name).find())
    result(matches, signals, sideEffect, frequency)
  }
}
