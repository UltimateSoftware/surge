// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.{ Materializer, QueueOfferResult }
import akka.stream.scaladsl.{ Sink, Source, SourceQueueWithComplete }
import org.slf4j.{ Logger, LoggerFactory }
import surge.health.{ HealthSignalStream, SignalHandler }
import surge.health.domain.HealthSignal

import scala.util.Try

trait StreamHandle {
  private var running: Boolean = true

  final def isRunning: Boolean = running

  def close(): Unit = {
    this.running = false
  }
}

object WindowingHealthSignalStream {
  val log: Logger = LoggerFactory.getLogger(getClass)
}

trait WindowingHealthSignalStream extends HealthSignalStream with ReleasableStream {
  import WindowingHealthSignalStream._
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
        Source
          .single(signal)
          .runWith(
            Sink.foreach(s =>
              queue
                .offer(s)
                .map {
                  case QueueOfferResult.Enqueued =>
                    log.debug("signal enqueued {}", s)
                  case QueueOfferResult.Dropped =>
                    log.warn("signal dropped {}", s)
                  case QueueOfferResult.Failure(cause) =>
                    log.error("signal failed {}", s, cause)
                  case QueueOfferResult.QueueClosed =>
                    log.debug("queue closed")
                }(actorSystem.dispatcher)))(Materializer(actorSystem))
      })
      Done
    }
  }
}
