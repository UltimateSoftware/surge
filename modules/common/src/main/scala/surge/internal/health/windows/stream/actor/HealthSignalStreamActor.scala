// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream.actor

import akka.actor.Actor
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcher
import surge.health.windows.{ AddedToWindow, Window, WindowAdvanced, WindowClosed, WindowOpened, WindowStopped, WindowStreamListener }
import surge.internal.health.windows._
import surge.internal.health.HealthSignalBusInternal

object HealthSignalStreamActor {
  def apply(
      signalBus: HealthSignalBusInternal,
      filters: Seq[SignalPatternMatcher],
      windowListener: Option[WindowStreamListener] = None): HealthSignalStreamActor = {
    new HealthSignalStreamActor(signalBus, filters, windowListener)
  }
}

case object Stop
protected class HealthSignalStreamActor(
    signalBus: HealthSignalBusInternal,
    filters: Seq[SignalPatternMatcher],
    val windowListener: Option[WindowStreamListener] = None)
    extends Actor
    with WindowStreamListener {
  override def receive: Receive = {
    case WindowAdvanced(w, data) => slide(w, data.signals)
    case WindowClosed(w, data)   => windowClosed(w.asInstanceOf[Window], data.signals)
    case WindowOpened(w)         => windowOpened(w.asInstanceOf[Window])
    case WindowStopped(w)        => windowStopped(w.asInstanceOf[Option[Window]])
    case AddedToWindow(d, w)     => dataAddedToWindow(d.asInstanceOf[HealthSignal], w.asInstanceOf[Window])

    case Stop => context.stop(self)
    case other =>
      unhandled(other)
  }

  override def windowClosed(window: Window, signals: Seq[HealthSignal]): Unit = {
    windowListener.foreach(w => w.windowClosed(window, signals))
  }

  override def windowOpened(window: Window): Unit = {
    windowListener.foreach(w => w.windowOpened(window))
  }

  override def windowStopped(maybeWindow: Option[Window]): Unit = {
    windowListener.foreach(w => w.windowStopped(maybeWindow))
  }

  override def windowAdvanced(window: Window, data: Seq[HealthSignal]): Unit = {
    windowListener.foreach(w => w.windowAdvanced(window, data))
  }

  override def dataAddedToWindow(data: HealthSignal, window: Window): Unit = {
    windowListener.foreach(w => w.dataAddedToWindow(data, window))
  }

  def slide(window: Window, data: Seq[HealthSignal]): Unit = {
    windowListener.foreach(w => w.windowAdvanced(window, data))
  }
}
