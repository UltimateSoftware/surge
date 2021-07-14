// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream.sliding

import akka.Done
import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, Sink, Source, SourceQueue, SourceQueueWithComplete }
import org.slf4j.{ Logger, LoggerFactory }
import surge.health.config.{ ThrottleConfig, WindowingStreamConfig }
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcher
import surge.health.windows.{ WindowEvent, WindowStreamListener }
import surge.health.{ HealthSignalListener, SignalHandler, SourcePlusQueue, SourceQueueBackedSignalHandler }
import surge.internal.health._
import surge.internal.health.windows._
import surge.internal.health.windows.actor.{ HealthSignalWindowActor, HealthSignalWindowActorRef }
import surge.internal.health.windows.stream.actor.HealthSignalStreamActor
import surge.internal.health.windows.stream.{ SignalPatternMatchResultHandler, SourcePlusQueueWithStreamHandle, StreamHandle, WindowingHealthSignalStream }

import scala.concurrent.Future

trait SlidingHealthSignalStream extends WindowingHealthSignalStream

object SlidingHealthSignalStream {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def apply(
      slidingConfig: WindowingStreamConfig,
      signalBus: HealthSignalBusInternal,
      filters: Seq[SignalPatternMatcher],
      streamMonitoringRef: Option[StreamMonitoringRef] = None,
      actorSystem: ActorSystem): SlidingHealthSignalStream = {
    val listener: WindowStreamListener =
      SignalPatternMatchResultHandler.asListener(signalBus, filters, streamMonitoringRef.map(r => r.actor))
    val ref = actorSystem.actorOf(Props(HealthSignalStreamActor(Some(listener))), name = "slidingHealthSignalStreamActor")
    new SlidingHealthSignalStreamImpl(slidingConfig, signalBus, filters, ref, actorSystem)
  }
}

class SlidingHealthSignalStreamHandle(
    windowActors: Seq[HealthSignalWindowActorRef],
    cleanUp: () => Unit,
    sourceQueue: Option[SourceQueueWithComplete[HealthSignal]])
    extends StreamHandle {
  override def close(): Unit = {
    sourceQueue.foreach(q => {
      q.complete()
    })
    super.close()
    cleanUp.apply()
    windowActors.foreach(actor => actor.stop())
  }
}

object WindowEventInterceptorActor {
  val log: Logger = LoggerFactory.getLogger(getClass)
}

// Actor interceptor for WindowEvents
private class WindowEventInterceptorActor(actorSystem: ActorSystem, backingActor: ActorRef, windowEventQueueProvider: () => Option[SourceQueue[WindowEvent]])
    extends Actor {

  override def receive: Receive = {
    case windowEvent: surge.health.windows.WindowEvent =>
      windowEventQueueProvider
        .apply()
        .foreach(queue => Source.single(windowEvent).runWith(Sink.foreach(event => queue.offer(event)))(Materializer(actorSystem)))
      backingActor ! windowEvent
    case other => backingActor ! other
  }
}

case class WindowingHealthSignalStreamState(windowHandle: Option[StreamHandle] = None, windowEvents: Option[SourceQueueWithComplete[WindowEvent]] = None)
private class SlidingHealthSignalStreamImpl(
    override val windowingConfig: WindowingStreamConfig,
    override val signalBus: HealthSignalBusInternal,
    override val filters: Seq[SignalPatternMatcher],
    override val underlyingActor: ActorRef,
    override val actorSystem: ActorSystem,
    state: WindowingHealthSignalStreamState = WindowingHealthSignalStreamState())
    extends SourceQueueBackedSignalHandler(actorSystem)
    with SlidingHealthSignalStream {
  import SlidingHealthSignalStream._

  // todo: consider storing these (windowHandle and windowEvents) in a State object and
  //  returning a copy of the SignalStream in all methods that alter state
  //  i.e. start, stop, subscribe, release, etc.
  private var windowHandle: StreamHandle = _
  private var windowEvents: Option[SourceQueueWithComplete[WindowEvent]] = None

  override def id(): String = "sliding-window-signal-listener"

  override protected[health] def windowEventsSourceQueue(): Option[SourceQueueWithComplete[WindowEvent]] = state.windowEvents

  override def subscribe(signalHandler: SignalHandler): HealthSignalListener = {
    if (!signalBus.subscriberInfo().exists(p => p.name == id())) {
      this.bindSignalHandler(signalHandler)
      // subscribe to bus
      signalBus.subscribe(subscriber = this, signalTopic)
    }
    this
  }

  override def signalHandler: SignalHandler = this

  override def start(maybeSideEffect: Option[() => Unit] = None): HealthSignalListener = {
    if (Option(windowHandle).exists(h => h.isRunning)) {
      this
    } else {
      val sourcePlusQueueWithStreamHandle: SourcePlusQueueWithStreamHandle[WindowEvent] = doWindowing(maybeSideEffect)
      this.windowHandle = sourcePlusQueueWithStreamHandle.streamHandle
      this
    }
  }

  override def stop(): HealthSignalListener = {
    release()
    this
  }

  override def windowEventSource(): SourcePlusQueue[WindowEvent] = {
    val sourcePlusQueue = super.windowEventSource()
    windowEvents = Some(sourcePlusQueue.queue)
    sourcePlusQueue
  }

  override def signalSource(buffer: Int, throttleConfig: ThrottleConfig): SourcePlusQueue[HealthSignal] = {
    val sourcePlusQueue = super.signalSource(buffer, throttleConfig)
    bindQueue(sourcePlusQueue.queue)

    sourcePlusQueue
  }

  override protected[health] def doWindowing(maybeSideEffect: Option[() => Unit]): SourcePlusQueueWithStreamHandle[WindowEvent] = {
    if (Option(windowHandle).exists(h => h.isRunning)) {
      throw new RuntimeException(
        "SlidingHealthSignalStream is already processing signals." +
          "Be sure to release the stream prior to processing again.")
    }

    // Create a windowActor for each configured windowing frequency
    val windowActors: Seq[HealthSignalWindowActorRef] = createWindowActors(windowEventSourceProvider = () => windowEvents)
    // HealthSignal Source
    val sourcePlusQueue = signalSource(windowingConfig.maxWindowSize, windowingConfig.throttleConfig)

    maybeSideEffect.foreach(m => m.apply())

    // WindowEvent Source
    val windowQueue = windowEventSource()

    // todo: handle pattern matching here instead of nesting away behind an actor (HealthSignalStreamActor) that simply
    //  delegates to SignalPatternMatchResultHandler.asListener
    windowQueue.source.runWith[Future[Done]](Sink.foreach(event => {
      log.trace("WindowEvent {}", event)
    }))(Materializer(actorSystem))

    // Source the HealthSignals and organize signals in windows
    sourcePlusQueue.source
      .toMat(Sink.foreach(signal => {
        windowActors.foreach(actor => actor.processSignal(signal))
      }))(Keep.left)
      .run()(Materializer(actorSystem))

    // Stream Handle
    val signalDataCleanup = () => {
      sourcePlusQueue.queue.complete()
      windowQueue.queue.complete()
      unbindQueue()
    }

    SourcePlusQueueWithStreamHandle[WindowEvent](windowQueue, new SlidingHealthSignalStreamHandle(windowActors, signalDataCleanup, Some(sourcePlusQueue.queue)))
  }

  override def release(): Unit = {
    releaseWindowHandle()
    underlyingActor ! surge.internal.health.windows.stream.actor.Stop
  }

  protected[health] def releaseWindowHandle(): Unit = {
    Option(this.windowHandle).foreach(h => h.close())
  }

  private def createWindowActors(windowEventSourceProvider: () => Option[SourceQueue[WindowEvent]]): Seq[HealthSignalWindowActorRef] = {
    val advancerConfig = windowingConfig.advancerConfig

    val windowEventInterceptorActor: ActorRef =
      actorSystem.actorOf(Props(new WindowEventInterceptorActor(actorSystem, underlyingActor, () => windowEvents)))

    windowingConfig.frequencies
      .map(freq =>
        HealthSignalWindowActor(
          actorSystem,
          initialWindowProcessingDelay = windowingConfig.windowingDelay,
          windowFrequency = freq,
          WindowSlider(advancerConfig.advanceAmount, advancerConfig.buffer)))
      .map(ref => ref.start(Some(windowEventInterceptorActor)))
  }
}
