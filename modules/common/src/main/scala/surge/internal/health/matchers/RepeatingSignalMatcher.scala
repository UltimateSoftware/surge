// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.matchers

import surge.health.domain.HealthSignalSource
import surge.health.matchers.{ SideEffect, SignalPatternMatchResult, SignalPatternMatcher, SignalPatternMatcherWithSideEffectBuilder }

import scala.concurrent.duration.FiniteDuration

/**
 * RepeatingSignalMatcher is responsible for detecting repeated signals in a HealthSignalStream. An atomicSignal matcher is provided to match on a single
 * signal. If the atomicSignalMatcher matches {{times}} times then the RepeatingSignalMatcher will capture the matches.
 * @param times
 *   Int - The number of repeats that are expected.
 * @param atomicMatcher
 *   SignalPatternMatcher
 * @param sideEffect
 *   Option[SideEffect]
 */
case class RepeatingSignalMatcher(times: Int, atomicMatcher: SignalPatternMatcher, sideEffect: Option[SideEffect])
    extends SignalPatternMatcherWithSideEffectBuilder {

  def withSideEffect(sideEffect: SideEffect): RepeatingSignalMatcher = copy(sideEffect = Some(sideEffect))

  override def searchForMatch(signalSource: HealthSignalSource, frequency: FiniteDuration): SignalPatternMatchResult = {
    val results = atomicMatcher.searchForMatch(signalSource, frequency)

    result(results.matches, signalSource.signals(), sideEffect, frequency)
  }
}
