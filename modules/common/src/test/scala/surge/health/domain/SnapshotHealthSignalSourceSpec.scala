// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.domain

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.wordspec.AnyWordSpec
import surge.health.SignalType

class SnapshotHealthSignalSourceSpec extends AnyWordSpec {

  "Not flush" in {
    val snapshot = new SnapshotHealthSignalSource(data = Seq(HealthSignal("testTopic", "testSignal", SignalType.TRACE,
      Trace("desc", None, None), Map.empty[String, String], None)))

    snapshot.flush()

    snapshot.signals().size shouldEqual 1
  }
}
