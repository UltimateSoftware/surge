// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams.replay

import org.apache.kafka.common.TopicPartition

case class ReplayProgress(partitionResults: Map[TopicPartition, Boolean] = Map.empty) {
  // todo: implement.
  def isComplete: Boolean = {
    percentComplete() >= 100.0
    //true
  }

  def percentComplete(): Double = {
    (partitionResults.values.count(result => result)/partitionResults.values.size)*100.0
  }
}

case object ReplayStarted
case class ResetComplete()
case object ReplayComplete

case object GetReplayProgress
