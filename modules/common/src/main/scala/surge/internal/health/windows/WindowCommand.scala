// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows

import surge.health.domain.HealthSignal
import surge.health.windows.Window

sealed trait WindowCommand {
  def w: Window
}

case class OpenWindow(w: Window, initialSignal: Option[HealthSignal]) extends WindowCommand
case class CloseWindow(w: Window, advance: Boolean = false) extends WindowCommand
case class AddToWindow(sig: HealthSignal, w: Window) extends WindowCommand
case class AdvanceWindow(w: Window, to: Window) extends WindowCommand

case class CloseCurrentWindow()
