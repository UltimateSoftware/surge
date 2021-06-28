// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream.sliding

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.stream.scaladsl.{ Keep, Sink, Source, SourceQueueWithComplete }
import akka.stream.{ Materializer, OverflowStrategy }
import org.slf4j.{ Logger, LoggerFactory }
import surge.health.config.WindowingStreamConfig
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcher
import surge.health.windows.WindowStreamListener
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

private class SlidingHealthSignalStreamImpl(
    windowingConfig: WindowingStreamConfig,
    override val signalBus: HealthSignalBusInternal,
    override val filters: Seq[SignalPatternMatcher],
    override val underlyingActor: ActorRef,
    override val actorSystem: ActorSystem)
    extends SlidingHealthSignalStream {

  private var windowHandle: StreamHandle = _
  private var signalData: SourceQueueWithComplete[HealthSignal] = _

  override def id(): String = "sliding-window-signal-listener"

  override protected[health] def sourceQueue(): Option[SourceQueueWithComplete[HealthSignal]] = Option(signalData)

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

  final override protected[health] def processWindows(): StreamHandle = {
    if (Option(windowHandle).exists(h => h.isRunning)) {
      throw new RuntimeException(
        "SlidingHealthSignalStream is already processing signals." +
          "Be sure to release the stream prior to processing again.")
    }

    // Create a windowActor for each configured windowing frequency
    val windowActors: Seq[HealthSignalWindowActorRef] = createWindowActors()

    signalData = Source
      .queue[HealthSignal](windowingConfig.maxStreamSize, OverflowStrategy.backpressure)
      .toMat(Sink.foreach(signal => {
        windowActors.foreach(actor => actor.processSignal(signal))
      }))(Keep.left)
      .run()(Materializer(actorSystem))

    // Stream Handle
    val signalDataCleanup = () => { signalData = null }
    new SlidingHealthSignalStreamHandle(windowActors, signalDataCleanup, sourceQueue())
  }

  override def release(): Unit = {
    releaseWindowHandle()
    // revisit - shouldn't creator stop this actor?
    underlyingActor ! surge.internal.health.windows.stream.actor.Stop
  }

  protected[health] def releaseWindowHandle(): Unit = {
    Option(this.windowHandle).foreach(h => h.close())
  }

  private def createWindowActors(): Seq[HealthSignalWindowActorRef] = {
    val advancerConfig = windowingConfig.advancerConfig
    windowingConfig.frequencies
      .map(freq => HealthSignalWindowActor(actorSystem, freq, WindowSlider(advancerConfig.advanceAmount, advancerConfig.buffer)))
      .map(ref => ref.start(Some(underlyingActor)))
  }
}
