// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows

import java.time.Instant
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import surge.health.SignalType
import surge.health.domain.{HealthSignal, Trace}
import surge.health.windows.Window

import scala.concurrent.duration._

class WindowSpec extends AnyWordSpec with Matchers with Eventually {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(1, Seconds)))
  "Window" should {
    "expire" in {
      val window = Window.windowFor(Instant.now(), duration = 1.second, control = None)
      eventually {
        window.expired() shouldEqual true
      }
    }

    "return snapshot data" in {
      val window = Window.windowFor(Instant.now(), duration = 1.second, control = None)

      window.snapShotData() shouldEqual window.priorData

      val windowWithData = Window(from = Instant.now().toEpochMilli,
        to = Instant.now().toEpochMilli, data = Seq(HealthSignal("testTopic", "testSignal", SignalType.TRACE,
        Trace("desc", None, None), Map.empty[String, String], None)), duration = 10.seconds,
        control = None, priorData = Seq.empty)

      windowWithData.snapShotData() shouldEqual windowWithData.data

      windowWithData.signals() shouldEqual windowWithData.data
    }
  }
}
