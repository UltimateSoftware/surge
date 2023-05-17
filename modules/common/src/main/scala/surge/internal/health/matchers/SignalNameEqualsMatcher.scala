// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.health.matchers

import surge.health.domain.HealthSignalSource
import surge.health.matchers.{ SideEffect, SignalPatternMatchResult, SignalPatternMatcher }

import java.util.UUID
import scala.concurrent.duration.FiniteDuration

/**
 * SignalNameMatcher is responsible for detecting signals in the HealthSignalStream that match a specified name.
 * @param name
 *   String
 * @param sideEffect
 *   Option[SideEffect]
 */
case class SignalNameEqualsMatcher(name: String, sideEffect: Option[SideEffect] = None) extends SignalPatternMatcher {
  def withSideEffect(sideEffect: SideEffect): SignalNameEqualsMatcher = copy(sideEffect = Some(sideEffect))

  override def searchForMatch(signalSource: HealthSignalSource, frequency: FiniteDuration): SignalPatternMatchResult = {
    val matches = signalSource.signals().filter(signal => signal.name == name)
    result(matches, signalSource.signals(), sideEffect, frequency)
  }
}
