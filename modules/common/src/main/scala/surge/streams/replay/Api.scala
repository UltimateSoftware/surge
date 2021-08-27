// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams.replay

import org.apache.kafka.common.TopicPartition

/**
 * ReplayProgress
 * @param partitionResults
 *   Map[TopicPartition, Boolean] tracking which partitions have been completely replayed
 */
case class ReplayProgress(partitionResults: Map[TopicPartition, Boolean] = Map.empty) {
  def isComplete: Boolean = {
    percentComplete() >= 100.0
  }

  /**
   * Percentage of all partitions that have been replayed.
   * @return
   *   Double
   */
  def percentComplete(): Double = {
    (partitionResults.values.count(result => result) / partitionResults.values.size) * 100.0
  }
}

sealed trait ReplayLifecycleEvent
case object ReplayStarted extends ReplayLifecycleEvent
case class ResetComplete() extends ReplayLifecycleEvent
case object ReplayComplete extends ReplayLifecycleEvent

sealed trait ReplayRequest
case object GetReplayProgress extends ReplayRequest
