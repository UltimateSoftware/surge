// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PartitionAssignmentsSpec extends AnyWordSpec with Matchers {
  "HostPort" should {
    "Serialize to and deserialize from a ByteBuffer" in {
      val hostPort1 = HostPort("Host", 1)
      val hostPort2 = HostPort("SecondHost", 2)

      HostPort.fromByteBuffer(hostPort1.toByteBuffer) shouldEqual Some(hostPort1)
      HostPort.fromByteBuffer(hostPort2.toByteBuffer) shouldEqual Some(hostPort2)
    }
  }
}
