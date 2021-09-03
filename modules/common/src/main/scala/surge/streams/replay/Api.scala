// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams.replay

trait ReplayLifecycleCallbacks {
  def onReplayStarted(replayStarted: ReplayStarted): Unit
  def onReplayReadyForMonitoring(replayReady: ReplayReadyForMonitoring): Unit
  def onReplayProgress(replayProgress: ReplayProgress): Unit

  def onReplayComplete(replayComplete: ReplayComplete): Unit
  def onReplayFailed(replayFailed: ReplayFailed): Unit
}

sealed trait ReplayLifecycleEvent

object ReplayProgress {
  def start(): ReplayProgress = ReplayProgress()
  def complete(): ReplayProgress = ReplayProgress(percentComplete = 100.0)
}

/**
 * ReplayProgress
 * @param percentComplete
 *   Double
 */
case class ReplayProgress(percentComplete: Double = 0.0) extends ReplayLifecycleEvent {
  def isComplete: Boolean = percentComplete >= 100.0
}

case class ReplayStarted() extends ReplayLifecycleEvent
case class ReplayReadyForMonitoring() extends ReplayLifecycleEvent
case class ReplayComplete() extends ReplayLifecycleEvent
case class ReplayFailed(error: Throwable) extends ReplayLifecycleEvent

sealed trait ReplayRequest
case object GetReplayProgress extends ReplayRequest
