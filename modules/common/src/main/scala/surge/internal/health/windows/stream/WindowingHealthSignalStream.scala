// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream

import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.scaladsl.{ Source, SourceQueueWithComplete }
import akka.stream.{ Materializer, OverflowStrategy }
import org.slf4j.{ Logger, LoggerFactory }
import surge.health.config.{ HealthSupervisorConfig, WindowingStreamConfig }
import surge.health.windows.WindowEvent
import surge.health.{ HealthSignalStream, SignalSourceQueueProvider, SourcePlusQueue }

trait StreamHandle {
  private var running: Boolean = true

  final def isRunning: Boolean = running

  def close(): Unit = {
    this.running = false
  }
}

case class SourcePlusQueueWithStreamHandle[T](sourcePlusQueue: SourcePlusQueue[T], streamHandle: StreamHandle)

object WindowingHealthSignalStream {
  val log: Logger = LoggerFactory.getLogger(getClass)
}

trait WindowingHealthSignalStream extends HealthSignalStream with SignalSourceQueueProvider with ReleasableStream {
  def underlyingActor: ActorRef
  def actorSystem: ActorSystem
  def windowingConfig: WindowingStreamConfig

  /**
   * Start a long running task to process data in windows
   * @return
   *   SourcePlusQueueWithStreamHandle used for closing the stream
   */
  protected[health] def doWindowing(maybeSideEffect: Option[() => Unit]): SourcePlusQueueWithStreamHandle[WindowEvent]

  /**
   * Provide WindowEvent(s) in a Source for stream operations
   * @return
   *   SourcePlusQueue[WindowEvent]
   */
  def windowEventSource(): SourcePlusQueue[WindowEvent] = {
    val eventSource = Source
      .queue[WindowEvent](windowingConfig.maxWindowSize, OverflowStrategy.backpressure)
      .throttle(windowingConfig.throttleConfig.elements, windowingConfig.throttleConfig.duration)

    val (sourceMat, source) = eventSource.preMaterialize()(Materializer(actorSystem))
    SourcePlusQueue(source, sourceMat)
  }

  /**
   * Provide SourceQueue of WindowEvents
   * @return
   *   Option[SourceQueueWithComplete]
   */
  protected[health] def windowEventsSourceQueue(): Option[SourceQueueWithComplete[WindowEvent]]
}
