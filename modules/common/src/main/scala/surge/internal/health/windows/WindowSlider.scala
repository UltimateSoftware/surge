// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows

import surge.health.domain.HealthSignal
import surge.health.windows.Window

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

trait WindowSlider extends Slider[Window] {
  override def slide(it: Window, amount: Int = 1, force: Boolean = false): Option[Window]
}

object WindowSlider {
  def apply(slideAmount: Int, bufferSize: Int): WindowSlider =
    new WindowSliderImpl(slideAmount, bufferSize)
}

private class WindowSliderImpl(override val slideAmount: Int, override val buffer: Int) extends WindowSlider {
  override def slide(it: Window, amount: Int, force: Boolean): Option[Window] = {
    val now = Instant.now()
    val duration: FiniteDuration = it.duration

    val newData = dropData(it, amount)

    if (force || it.data.size >= buffer) {
      Some(it.copy(from = now.toEpochMilli, to = now.plusMillis(duration.toMillis).toEpochMilli, duration = duration, data = newData))
    } else {
      None
    }
  }

  private def dropData(it: Window, amount: Int): Seq[HealthSignal] = {
    it.data.drop(amount)
  }
}
