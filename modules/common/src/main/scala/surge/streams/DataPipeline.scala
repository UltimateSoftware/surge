// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams

import surge.streams.DataPipeline.ReplayResult
import surge.streams.replay.ReplayControl

import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

trait DataPipeline {
  def start(): Unit
  def stop(): Unit
  def replay(): Future[ReplayResult]
  def replayWithCompletionStage(): CompletionStage[ReplayResult] = {
    replay().toJava
  }

  def getReplayControl: ReplayControl
}

object DataPipeline {
  sealed trait ReplayResult {
    def replayId: String
  }
  // This is a case class on purpose, Kotlin doesn't do pattern matching against scala case objects :(
  case class ReplaySuccessfullyStarted(replayId: String) extends ReplayResult
  case class ReplayFailed(replayId: String, reason: Throwable) extends ReplayResult

  case class ReplayProgress(replayId: String, percentComplete: Double) extends ReplayResult
}

class TypedDataPipeline[Type](dataPipeline: DataPipeline) extends DataPipeline {
  override def start(): Unit = dataPipeline.start()
  override def stop(): Unit = dataPipeline.stop()
  override def replay(): Future[ReplayResult] = dataPipeline.replay()
  override def getReplayControl: ReplayControl = dataPipeline.getReplayControl
}
