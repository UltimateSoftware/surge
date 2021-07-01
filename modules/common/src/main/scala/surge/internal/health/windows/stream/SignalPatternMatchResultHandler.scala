// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream

import akka.actor.ActorRef
import org.slf4j.{ Logger, LoggerFactory }
import surge.health.domain.HealthSignal
import surge.health.matchers.{ SideEffect, SideEffectBuilder, SignalPatternMatchResult, SignalPatternMatcher }
import surge.health.windows.{ AddedToWindow, Window, WindowAdvanced, WindowClosed, WindowData, WindowOpened, WindowStopped, WindowStreamListener }
import surge.internal.health._

trait SignalPatternMatchResultHandler {
  def injectSignalPatternMatchResultIntoStream(result: SignalPatternMatchResult): Unit
}

object SignalPatternMatchResultHandler {
  val log: Logger = LoggerFactory.getLogger(getClass)
  def asListener(
      signalBus: HealthSignalBusInternal,
      filters: Seq[SignalPatternMatcher],
      monitoringRef: Option[ActorRef] = None): SignalPatternMatchResultHandler with WindowStreamListener = {
    new SignalPatternMatchResultHandlerImpl(signalBus, filters, monitoringRef)
  }
}

private class SignalPatternMatchResultHandlerImpl(signalBus: HealthSignalBusInternal, filters: Seq[SignalPatternMatcher], monitoringRef: Option[ActorRef])
    extends SignalPatternMatchResultHandler
    with WindowStreamListener {
  import SignalPatternMatchResultHandler._

  override def windowAdvanced(it: Window, data: Seq[HealthSignal]): Unit = {
    log.trace(s"Window tumbled $it")
    val all = filters.map(f => f.searchForMatch(data, it.duration))
    val freq = it.duration

    val result = SignalPatternMatchResult(Seq.empty, data, SideEffect(Seq()), freq)

    val sideEffectBuilder = SideEffectBuilder()
    all.foreach(r => {
      r.sideEffect.signals.foreach(s => {
        sideEffectBuilder.addSideEffectSignal(s)
      })
    })

    injectSignalPatternMatchResultIntoStream(result.copy(sideEffect = sideEffectBuilder.buildSideEffect()))
    monitoringRef.foreach(m => m ! WindowAdvanced(it, WindowData(data, freq)))
  }

  override def windowOpened(window: Window): Unit = {
    monitoringRef.foreach(m => m ! WindowOpened(window))
  }

  override def windowStopped(window: Option[Window]): Unit = {
    monitoringRef.foreach(m => m ! WindowStopped(window))
  }

  override def windowClosed(window: Window, data: Seq[HealthSignal]): Unit = {
    val all = filters.map(f => f.searchForMatch(data, window.duration))

    val freq = window.duration
    val result = SignalPatternMatchResult(Seq.empty, data, SideEffect(Seq()), freq)

    val sideEffectBuilder = SideEffectBuilder()
    all.foreach(r => {
      r.sideEffect.signals.foreach(s => {
        sideEffectBuilder.addSideEffectSignal(s)
      })
    })

    injectSignalPatternMatchResultIntoStream(result.copy(sideEffect = sideEffectBuilder.buildSideEffect()))
    monitoringRef.foreach(m => m ! WindowClosed(window, WindowData(data, freq)))
  }

  override def dataAddedToWindow(data: HealthSignal, window: Window): Unit = {
    monitoringRef.foreach(m => m ! AddedToWindow(data, window))
  }

  override def injectSignalPatternMatchResultIntoStream(result: SignalPatternMatchResult): Unit = {
    result.sideEffect.signals.foreach(s => {
      signalBus.publish(s.copy())
    })
  }
}
