// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.config

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._

class WindowingStreamConfigLoaderSpec extends AnyWordSpec with Matchers {

  "WindowingStreamConfig" should {
    "load" in {
      val config = ConfigFactory.load("windowing-stream-config-loader-spec").getConfig("surge.health.window.stream")

      val windowingStreamConfig = WindowingStreamConfigLoader.load(config)

      windowingStreamConfig.advancerConfig.advanceAmount shouldEqual 2
      windowingStreamConfig.advancerConfig.buffer shouldEqual 15

      windowingStreamConfig.windowingDelay shouldEqual 10.seconds
      windowingStreamConfig.maxWindowSize shouldEqual 200
      windowingStreamConfig.frequencies shouldEqual Seq(20.seconds)
    }
  }
}
