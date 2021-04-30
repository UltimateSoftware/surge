// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import org.apache.kafka.common.TopicPartition
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.kafka.HostPort

class SubscriptionInfoSpec extends AnyWordSpec with Matchers {
  "AssignmentInfo" should {
    "Result in the same info after encoding/decoding" in {
      val assignmentInfo = AssignmentInfo(List(
        new TopicPartition("test-topic", 0) -> new HostPort("localhost", 123),
        new TopicPartition("test-topic", 1) -> new HostPort("localhost", 123),
        new TopicPartition("test-topic2", 0) -> new HostPort("localhost", 456)))

      AssignmentInfo.decode(assignmentInfo.encode) shouldEqual Some(assignmentInfo)
    }
  }
}
