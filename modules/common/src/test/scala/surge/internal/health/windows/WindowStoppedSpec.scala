// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.wordspec.AnyWordSpec
import surge.health.windows.WindowStopped

class WindowStoppedSpec extends AnyWordSpec {

  "return None when window not set" in {
    val w = WindowStopped(w = None)

    w.window() shouldEqual None
  }
}
