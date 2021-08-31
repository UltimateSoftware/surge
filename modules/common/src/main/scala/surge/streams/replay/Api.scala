// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams.replay

/**
 * ReplayProgress
 * @param percentComplete
 *   Double
 */
case class ReplayProgress(percentComplete: Double = 0.0) {
  def isComplete: Boolean = percentComplete >= 100.0
}

sealed trait ReplayLifecycleEvent
case class ReplayStarted() extends ReplayLifecycleEvent
case class ReplayReady() extends ReplayLifecycleEvent
case object ReplayComplete extends ReplayLifecycleEvent

sealed trait ReplayRequest
case object GetReplayProgress extends ReplayRequest
