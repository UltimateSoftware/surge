// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.matchers

import surge.health.domain.{ HealthSignal, HealthSignalSource }

import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration

final case class SignalPatternMatchResult(
    matches: Seq[HealthSignal],
    existingSignals: Seq[HealthSignal],
    sideEffect: SideEffect,
    frequency: FiniteDuration,
    signalSource: Option[HealthSignalSource]) {
  def found(): Boolean = matches.nonEmpty

  def hasSideEffects: Boolean = sideEffect.signals.nonEmpty
}

trait SignalPatternMatcher {
  def sideEffect(): Option[SideEffect]
  def searchForMatch(signalSource: HealthSignalSource, frequency: FiniteDuration): SignalPatternMatchResult

  def result(matches: Seq[HealthSignal], signals: Seq[HealthSignal], sideEffect: Option[SideEffect], frequency: FiniteDuration): SignalPatternMatchResult = {
    if (matches.nonEmpty) {
      SignalPatternMatchResult(matches, signals, sideEffect.getOrElse(SideEffect(Seq())), frequency, None)
    } else {
      SignalPatternMatchResult(matches, signals, SideEffect(Seq()), frequency, None)
    }
  }
}

case class SideEffect(signals: Seq[HealthSignal])

object SideEffectBuilder {
  def apply(): SideEffectBuilder = new SideEffectBuilderImpl
}

trait SideEffectBuilder {
  def addSideEffect(sideEffect: SideEffect): SideEffectBuilder
  def addSideEffectSignal(signal: HealthSignal): SideEffectBuilder
  def buildSideEffect(): SideEffect
  def reset(): SideEffectBuilder
}

private[health] class SideEffectBuilderImpl extends SideEffectBuilder {
  private val signals: ArrayBuffer[HealthSignal] = new ArrayBuffer[HealthSignal]

  override def addSideEffect(sideEffect: SideEffect): SideEffectBuilder = {
    sideEffect.signals.foreach(s => signals.append(s))
    this
  }
  override def addSideEffectSignal(signal: HealthSignal): SideEffectBuilder = {
    signals.append(signal)
    this
  }

  override def reset(): SideEffectBuilder = {
    signals.clear()
    this
  }

  override def buildSideEffect(): SideEffect = SideEffect(signals.toList)
}

abstract class SignalPatternMatcherWithSideEffectBuilder extends SideEffectBuilderImpl with SignalPatternMatcher
