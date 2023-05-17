// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.windows

import akka.actor.NoSerializationVerificationNeeded
import surge.health.domain.HealthSignal

sealed trait StreamProcessingEvent extends NoSerializationVerificationNeeded
trait WindowEvent extends StreamProcessingEvent {
  def window(): Option[Window]
}

case class WindowAdvanced(w: Window, d: WindowData) extends WindowEvent {
  override def window(): Option[Window] = Some(w)
}

case class WindowClosed(w: Window, d: WindowData) extends WindowEvent {
  override def window(): Option[Window] = Some(w)
}

case class WindowOpened(w: Window) extends WindowEvent {
  override def window(): Option[Window] = Some(w)
}

case class AddedToWindow(s: HealthSignal, w: Window) extends WindowEvent {
  override def window(): Option[Window] = Some(w)
}

case class WindowStopped(w: Option[Window]) extends WindowEvent {
  override def window(): Option[Window] = w
}

case class WindowPaused(w: Window) extends WindowEvent {
  override def window(): Option[Window] = Some(w)
}

case class WindowResumed(w: Window) extends WindowEvent {
  override def window(): Option[Window] = Some(w)
}
