// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.windows

import surge.health.domain.HealthSignal

trait WindowStreamListener {
  def windowOpened(window: Window): Unit
  def windowClosed(window: Window, data: Seq[HealthSignal]): Unit
  def windowAdvanced(window: Window, data: Seq[HealthSignal]): Unit

  def windowStopped(window: Option[Window]): Unit

  def dataAddedToWindow(data: HealthSignal, window: Window): Unit
}
