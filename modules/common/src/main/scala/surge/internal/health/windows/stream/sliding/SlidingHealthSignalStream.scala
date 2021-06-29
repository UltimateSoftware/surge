// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream.sliding

import akka.NotUsed
import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source, SourceQueue, SourceQueueWithComplete }
import akka.stream.{ Materializer, OverflowStrategy }
import org.slf4j.{ Logger, LoggerFactory }
import surge.health.config.WindowingStreamConfig
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcher
import surge.health.windows.{ WindowEvent, WindowStreamListener }
import surge.health.{ HealthSignalListener, SignalHandler }
import surge.internal.health._
import surge.internal.health.windows._
import surge.internal.health.windows.actor.{ HealthSignalWindowActor, HealthSignalWindowActorRef }
import surge.internal.health.windows.stream.actor.HealthSignalStreamActor
import surge.internal.health.windows.stream.{ SignalPatternMatchResultHandler, StreamHandle, WindowingHealthSignalStream }

trait SlidingHealthSignalStream extends WindowingHealthSignalStream

object SlidingHealthSignalStream {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def apply(
      slidingConfig: WindowingStreamConfig,
      signalBus: HealthSignalBusInternal,
      filters: Seq[SignalPatternMatcher],
      streamMonitoringRef: Option[StreamMonitoringRef] = None,
      actorSystem: ActorSystem): SlidingHealthSignalStream = {
    val listener: WindowStreamListener = SignalPatternMatchResultHandler.asListener(signalBus, filters, streamMonitoringRef.map(r => r.actor))
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
class WindowEventInterceptorActor(actorSystem: ActorSystem, backingActor: ActorRef, windowEventQueueProvider: () => Option[SourceQueue[WindowEvent]])
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

private class SlidingHealthSignalStreamImpl(
    windowingConfig: WindowingStreamConfig,
    override val signalBus: HealthSignalBusInternal,
    override val filters: Seq[SignalPatternMatcher],
    override val underlyingActor: ActorRef,
    override val actorSystem: ActorSystem)
    extends SlidingHealthSignalStream {

  private var windowHandle: StreamHandle = _
  private var signalData: Option[SourceQueueWithComplete[HealthSignal]] = None

  private var windowEvents: Option[SourceQueueWithComplete[WindowEvent]] = None

  override def id(): String = "sliding-window-signal-listener"

  override protected[health] def signalSourceQueue(): Option[SourceQueueWithComplete[HealthSignal]] = signalData
  override protected[health] def windowEventsSourceQueue(): Option[SourceQueueWithComplete[WindowEvent]] = windowEvents

  override def subscribeWithFilters(signalHandler: SignalHandler, filters: Seq[SignalPatternMatcher] = Seq.empty): HealthSignalListener = {
    if (!signalBus.subscriberInfo().exists(p => p.name == id())) {
      this.bindSignalHandler(signalHandler)
      // subscribe to bus
      signalBus.subscribe(subscriber = this, signalTopic)
    }
    this
  }

  override def start(maybeSideEffect: Option[() => Unit] = None): HealthSignalListener = {
    if (Option(windowHandle).exists(h => h.isRunning)) {
      this
    } else {
      this.windowHandle = processWindows()
      maybeSideEffect.foreach(m => m())
      this
    }
  }

  override def stop(): HealthSignalListener = {
    release()
    this
  }

  override def windowEventSource(): Source[WindowEvent, NotUsed] = {
    val eventSource = Source
      .queue[WindowEvent](windowingConfig.maxWindowSize, OverflowStrategy.backpressure)
      .throttle(windowingConfig.throttleConfig.elements, windowingConfig.throttleConfig.duration)

    val (sourceMat, source) = eventSource.preMaterialize()(Materializer(actorSystem))

    windowEvents = Some(sourceMat)

    source
  }

  final override protected[health] def processWindows(): StreamHandle = {
    if (Option(windowHandle).exists(h => h.isRunning)) {
      throw new RuntimeException(
        "SlidingHealthSignalStream is already processing signals." +
          "Be sure to release the stream prior to processing again.")
    }

    // Create a windowActor for each configured windowing frequency
    val windowActors: Seq[HealthSignalWindowActorRef] = createWindowActors(windowEventSourceProvider = () => windowEvents)

    signalData = Some(
      Source
        .queue[HealthSignal](windowingConfig.maxWindowSize, OverflowStrategy.backpressure)
        .throttle(windowingConfig.throttleConfig.elements, windowingConfig.throttleConfig.duration)
        .toMat(Sink.foreach(signal => {
          windowActors.foreach(actor => actor.processSignal(signal))
        }))(Keep.left)
        .run()(Materializer(actorSystem)))

    // Stream Handle
    val signalDataCleanup = () => { signalData = None }
    new SlidingHealthSignalStreamHandle(windowActors, signalDataCleanup, signalSourceQueue())
  }

  override def release(): Unit = {
    releaseWindowHandle()
    // revisit - shouldn't creator stop this actor?
    underlyingActor ! surge.internal.health.windows.stream.actor.Stop
  }

  protected[health] def releaseWindowHandle(): Unit = {
    Option(this.windowHandle).foreach(h => h.close())
  }

  private def createWindowActors(windowEventSourceProvider: () => Option[SourceQueue[WindowEvent]]): Seq[HealthSignalWindowActorRef] = {
    val advancerConfig = windowingConfig.advancerConfig

    val windowEventInterceptorActor = actorSystem.actorOf(Props(new WindowEventInterceptorActor(actorSystem, underlyingActor, () => windowEvents)))

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
