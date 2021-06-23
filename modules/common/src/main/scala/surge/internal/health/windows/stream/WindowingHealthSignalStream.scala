// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream

import java.util.concurrent.ArrayBlockingQueue

import akka.Done
import akka.actor.ActorRef
import surge.health.{ HealthSignalStream, SignalHandler }
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcher
import surge.health.windows.Window

import scala.concurrent.duration._
import scala.util.Try

trait StreamHandle {
  def release(): Unit
}

case class RestartBackoff(minBackoff: FiniteDuration, maxBackoff: FiniteDuration, randomFactor: Double)

object WindowingHealthSignalStream {
  val defaultRestartBackoff: RestartBackoff = RestartBackoff(2.seconds, 5.seconds, 0.2)
}

trait WindowingHealthSignalStream extends HealthSignalStream with ReleasableStream {
  def underlyingActor: ActorRef
  def processWindows(filters: Seq[SignalPatternMatcher], monitoringActor: Option[ActorRef]): StreamHandle

  def signalAddedToWindow(signal: HealthSignal, window: Window): Unit = {}

  protected[health] def signals(): ArrayBlockingQueue[HealthSignal]

  /**
   * Add Signal to Blocking Queue for processing
   *
   * @return
   *   SignalHandler
   */
  override def signalHandler: SignalHandler = (signal: HealthSignal) => {
    // todo: consider ways to handle failed offer to add signal to blocking queue
    Try {
      if (!signals().add(signal)) {
        throw new RuntimeException("failed to add signal. need too handle this case")
      }
      Done
    }
  }
}
