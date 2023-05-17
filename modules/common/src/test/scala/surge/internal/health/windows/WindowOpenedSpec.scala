// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows

import org.mockito.Mockito.mock
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.wordspec.AnyWordSpec
import surge.health.windows.{Window, WindowOpened}

class WindowOpenedSpec extends AnyWordSpec {

  "return window when window is set" in {
    val mockWindow = mock(classOf[Window])
    val w = WindowOpened(w = mockWindow)

    w.window() shouldEqual Some(mockWindow)
  }
}
