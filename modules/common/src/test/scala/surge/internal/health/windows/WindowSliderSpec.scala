// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows

import java.time.Instant

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.FiniteDuration
import org.mockito.Mockito._
import surge.health.domain.HealthSignal
import surge.health.windows.Window
class WindowSliderSpec extends AnyWordSpec with Matchers {

  "WindowSlider" should {
    "properly create next window with correct duration" in {
      val window = Window
        .windowFor(Instant.now(), FiniteDuration(10, "seconds"), control = None)
        .copy(data = Seq(mock(classOf[HealthSignal]), mock(classOf[HealthSignal]), mock(classOf[HealthSignal])))
      val slider: WindowSlider = WindowSlider(slideAmount = 1, bufferSize = 0)

      val maybeNext = slider.advance(window)
      maybeNext.nonEmpty shouldEqual true

      maybeNext.get.duration shouldEqual window.duration
    }

    "advance 1 space" in {
      val slider: WindowSlider = WindowSlider(slideAmount = 1, bufferSize = 0)
      val window = Window
        .windowFor(Instant.now(), FiniteDuration(10, "seconds"), control = None)
        .copy(data = Seq(mock(classOf[HealthSignal]), mock(classOf[HealthSignal]), mock(classOf[HealthSignal])))
      val newWindow = slider.advance(window)
      newWindow.isDefined shouldEqual true
      newWindow.get.data.size shouldEqual 2
    }

    "only advance data.size elements given slide amount larger than current window" in {
      val slider: WindowSlider = WindowSlider(slideAmount = 10, bufferSize = 0)
      val window = Window
        .windowFor(Instant.now(), FiniteDuration(10, "seconds"), control = None)
        .copy(data = Seq(mock(classOf[HealthSignal]), mock(classOf[HealthSignal]), mock(classOf[HealthSignal])))
      val newWindow = slider.advance(window)
      newWindow.isDefined shouldEqual true
      newWindow.get.data.size shouldEqual 0
    }

    "not advance when slideAmount is negative" in {
      val slider: WindowSlider = WindowSlider(slideAmount = -1, bufferSize = 0)
      val window = Window
        .windowFor(Instant.now(), FiniteDuration(10, "seconds"), control = None)
        .copy(data = Seq(mock(classOf[HealthSignal]), mock(classOf[HealthSignal]), mock(classOf[HealthSignal])))
      val newWindow = slider.advance(window)
      newWindow.isDefined shouldEqual true
      newWindow.get.data.size shouldEqual 3
    }

    "not advance when slideAmount is zero" in {
      val slider: WindowSlider = WindowSlider(slideAmount = 0, bufferSize = 0)
      val window = Window
        .windowFor(Instant.now(), FiniteDuration(10, "seconds"), control = None)
        .copy(data = Seq(mock(classOf[HealthSignal]), mock(classOf[HealthSignal]), mock(classOf[HealthSignal])))
      val newWindow = slider.advance(window)
      newWindow.isDefined shouldEqual true
      newWindow.get.data.size shouldEqual 3
    }

    "not advance until buffer exceeded" in {
      val slider: WindowSlider = WindowSlider(slideAmount = 0, bufferSize = 5)
      val window = Window
        .windowFor(Instant.now(), FiniteDuration(10, "seconds"), control = None)
        .copy(data = Seq(mock(classOf[HealthSignal]), mock(classOf[HealthSignal]), mock(classOf[HealthSignal])))

      val shouldBeEmpty = slider.advance(window)
      shouldBeEmpty.nonEmpty shouldEqual false

      val shouldExist: Option[Window] =
        slider.advance(window.copy(data = window.data ++ Seq(mock(classOf[HealthSignal]), mock(classOf[HealthSignal]), mock(classOf[HealthSignal]))))

      shouldExist.nonEmpty shouldEqual true
    }
  }
}
