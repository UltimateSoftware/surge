// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows

import java.time.Instant

import com.typesafe.config.{ Config, ConfigFactory }
import surge.health.domain.HealthSignal
import surge.health.windows.Window

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait WindowSlider extends Slider[Window] {
  override def slide(it: Window, amount: Int = 1, force: Boolean = false): Option[Window]
}

object WindowSlider {
  val config: Config = ConfigFactory.load()
  private val configuredSlideAmount: Int = Try { config.getInt("surge.health.window.stream.slider.amount") }.getOrElse(1)
  private val configuredBufferSize: Int = Try { config.getInt("surge.health.window.stream.slider.buffer") }.getOrElse(10)

  def apply(slideAmount: Int = configuredSlideAmount, bufferSize: Int = configuredBufferSize): WindowSlider =
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
