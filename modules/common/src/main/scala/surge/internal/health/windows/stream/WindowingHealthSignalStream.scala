// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source, SourceQueueWithComplete }
import surge.health.{ HealthSignalStream, SignalHandler }
import surge.health.domain.HealthSignal
import surge.utils.BooleanWrapper

import scala.util.Try

trait StreamHandle {
  private val running: BooleanWrapper = new BooleanWrapper().asTrue()

  final def isRunning: Boolean = running.wrapped()

  def close(): Unit = {
    this.running.setFalse()
  }
}

trait WindowingHealthSignalStream extends HealthSignalStream with ReleasableStream {
  def underlyingActor: ActorRef
  def actorSystem: ActorSystem

  /**
   * Start a long running task to process data in windows
   * @return
   *   StreamHandle used for closing the stream
   */
  protected[health] def processWindows(): StreamHandle

  /**
   * Provide SourceQueue for processing
   * @return
   *   Option[SourceQueueWithComplete]
   */
  protected[health] def sourceQueue(): Option[SourceQueueWithComplete[HealthSignal]]

  /**
   * Add Signal to Source Queue for processing
   *
   * @return
   *   SignalHandler
   */
  final override def signalHandler: SignalHandler = (signal: HealthSignal) => {
    Try {
      sourceQueue().foreach(queue => {
        Source.single(signal).runWith(Sink.foreach(s => queue.offer(s)))(Materializer(actorSystem))
      })
      Done
    }
  }
}
