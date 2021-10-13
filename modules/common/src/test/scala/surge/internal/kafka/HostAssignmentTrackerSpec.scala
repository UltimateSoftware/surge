// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.kafka.HostPort

import scala.concurrent.ExecutionContext

class HostAssignmentTrackerSpec extends AnyWordSpec with Matchers with ScalaFutures {
  private implicit val ec: ExecutionContext = ExecutionContext.global

  private val topic1Partition0 = new TopicPartition("test1", 0)
  private val topic1Partition1 = new TopicPartition("test1", 1)
  private val topic1Partition2 = new TopicPartition("test1", 2)
  private val topic2Partition0 = new TopicPartition("test2", 0)
  private val topic2Partition1 = new TopicPartition("test2", 1)

  private val host1 = new HostPort("localhost", 10000)
  private val host2 = new HostPort("host2", 12345)
  private val host3 = new HostPort("thirdHost", 56789)
  "HostAssignmentTracker" should {
    "Update with new assignments" in {
      val initialState = Map(topic1Partition0 -> host1, topic1Partition1 -> host2, topic1Partition2 -> host3)
      HostAssignmentTracker.updateState(initialState)
      HostAssignmentTracker.allAssignments.futureValue shouldEqual initialState

      HostAssignmentTracker.updateState(topic1Partition2, host2)
      HostAssignmentTracker.getAssignment(topic1Partition2).futureValue shouldEqual Some(host2)
      HostAssignmentTracker.clearAllAssignments()
    }

    "Filter assignments for a particular topic" in {
      val state =
        Map(topic1Partition0 -> host1, topic1Partition1 -> host2, topic1Partition2 -> host3, topic2Partition0 -> host1, topic2Partition1 -> host2)

      HostAssignmentTracker.updateState(state)
      HostAssignmentTracker.getAssignments(topic2Partition0.topic()).futureValue shouldEqual Map(topic2Partition0 -> host1, topic2Partition1 -> host2)
      HostAssignmentTracker.clearAllAssignments()
    }
  }
}
