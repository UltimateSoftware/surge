// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream

import java.util.concurrent.ArrayBlockingQueue

import akka.Done
import akka.actor.ActorRef
import surge.health.{ HealthSignalStream, SignalHandler }
import surge.health.domain.HealthSignal
import surge.health.windows.Window

import scala.util.Try

trait StreamHandle {
  def close(): Unit
}

trait WindowingHealthSignalStream extends HealthSignalStream with ReleasableStream {
  def underlyingActor: ActorRef

  /**
   * Start a long running task to process data in windows
   * @return
   *   StreamHandle used for releasing the stream
   */
  def processWindows(): StreamHandle

  def signalAddedToWindow(signal: HealthSignal, window: Window): Unit = {}

  protected[health] def signals(): ArrayBlockingQueue[HealthSignal]

  /**
   * Add Signal to Blocking Queue for processing
   *
   * @return
   *   SignalHandler
   */
  override def signalHandler: SignalHandler = (signal: HealthSignal) => {
    Try {
      // wait for space to be available providing
      // naive back pressure on producer.
      signals().put(signal)
      Done
    }
  }
}
