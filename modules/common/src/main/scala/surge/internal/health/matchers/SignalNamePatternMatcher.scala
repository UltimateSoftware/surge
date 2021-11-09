// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.matchers

import surge.health.domain.HealthSignalSource
import surge.health.matchers.{ SideEffect, SignalPatternMatchResult, SignalPatternMatcher }

import java.util.UUID
import java.util.regex.Pattern
import scala.concurrent.duration.FiniteDuration

object SignalNamePatternMatcher {
  def endsWith(suffix: String): SignalNamePatternMatcher = {
    SignalNamePatternMatcher(pattern = Pattern.compile(suffix + "$"))
  }

  def beginsWith(prefix: String): SignalNamePatternMatcher = {
    SignalNamePatternMatcher(pattern = Pattern.compile("^" + prefix))
  }
}

case class SignalNamePatternMatcher(pattern: Pattern, sideEffect: Option[SideEffect] = None) extends SignalPatternMatcher {
  override def searchForMatch(signalSource: HealthSignalSource, frequency: FiniteDuration): SignalPatternMatchResult = {
    val matches = signalSource.signals().filter(signal => pattern.matcher(signal.name).find())
    result(matches, signalSource.signals(), sideEffect, frequency)
  }
}
