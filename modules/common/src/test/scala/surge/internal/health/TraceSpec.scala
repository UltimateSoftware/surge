// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.health.domain.Trace

class TraceSpec extends AnyWordSpec with Matchers {

  "Trace" should {
    "create copy with error" in {
      val trace = Trace("trace", None, None)

      val traceWithError = trace.withError(new RuntimeException("added error"))

      traceWithError.error.isDefined shouldEqual true
      traceWithError.error.get shouldBe a[RuntimeException]
      traceWithError.equals(trace) shouldEqual false
    }
  }
}
