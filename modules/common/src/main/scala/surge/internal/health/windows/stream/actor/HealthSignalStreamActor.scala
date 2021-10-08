// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream.actor

import akka.actor.{Actor, NoSerializationVerificationNeeded}
import surge.health.domain.HealthSignal
import surge.health.windows.{AddedToWindow, Window, WindowAdvanced, WindowClosed, WindowOpened, WindowStopped, WindowStreamListener}

object HealthSignalStreamActor {
  def apply(windowListener: Option[WindowStreamListener] = None): HealthSignalStreamActor = {
    new HealthSignalStreamActor(windowListener)
  }
}

case object Stop extends NoSerializationVerificationNeeded

protected class HealthSignalStreamActor(val windowListener: Option[WindowStreamListener] = None) extends Actor with WindowStreamListener {
  override def receive: Receive = {
    case WindowAdvanced(w, data) => slide(w, data.signals)
    case WindowClosed(w, data)   => windowClosed(w, data.signals)
    case WindowOpened(w)         => windowOpened(w)
    case WindowStopped(w)        => windowStopped(w)
    case AddedToWindow(d, w)     => dataAddedToWindow(d, w)

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
