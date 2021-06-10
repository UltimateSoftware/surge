// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows

import java.time.Instant

import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.wordspec.AnyWordSpec
import surge.health.windows.Window

import scala.concurrent.duration._
import scala.languageFeature.postfixOps

class WindowSpec extends AnyWordSpec with Matchers with Eventually {
  implicit val postOp: postfixOps = postfixOps

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(1, Seconds)))
  "Window" should {
    "expire" in {
      val window = Window.windowFor(Instant.now(), duration = 1 second)
      eventually {
        window.expired() shouldEqual true
      }
    }
  }
}
