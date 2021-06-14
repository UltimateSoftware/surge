// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream.sliding

import java.util.concurrent.{ArrayBlockingQueue, Callable, Executors}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.scaladsl.Source
import org.slf4j.{Logger, LoggerFactory}
import surge.health.config.WindowingStreamConfig
import surge.health.{HealthSignalListener, SignalHandler}
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcher
import surge.health.windows.WindowStreamListener
import surge.internal.health._
import surge.internal.health.windows._
import surge.internal.health.windows.actor.{HealthSignalWindowActor, HealthSignalWindowActorRef}
import surge.internal.health.windows.stream.actor.HealthSignalStreamActor
import surge.internal.health.windows.stream.{SignalPatternMatchResultHandler, StreamHandle, WindowingHealthSignalStream}

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

private class SlidingHealthSignalStreamImpl(
    windowingConfig: WindowingStreamConfig,
    override val signalBus: HealthSignalBusInternal,
    override val filters: Seq[SignalPatternMatcher],
    override val underlyingActor: ActorRef,
    actorSystem: ActorSystem)
    extends SlidingHealthSignalStream {

  private val fairQueueAccess: Boolean = true

  private var windowHandle: StreamHandle = _

  override protected[health] val signals: ArrayBlockingQueue[HealthSignal] =
    new ArrayBlockingQueue[HealthSignal](windowingConfig.maxStreamSize, fairQueueAccess)

  override def id(): String = "sliding-window-signal-listener"

  override def subscribeWithFilters(signalHandler: SignalHandler, filters: Seq[SignalPatternMatcher] = Seq.empty): HealthSignalListener = {
    if (!signalBus.subscriberInfo().exists(p => p.name == id())) {
      this.bindSignalHandler(signalHandler)
      // subscribe to bus
      signalBus.subscribe(subscriber = this, signalTopic)
    }
    this
  }

  override def start(maybeSideEffect: Option[() => Unit] = None): HealthSignalListener = {
    Option(windowHandle) match {
      case Some(_) =>
        this
      case None =>
        this.windowHandle = processWindows(filters, Some(underlyingActor))
        maybeSideEffect.foreach(m => m())
        this
    }
  }

  override def stop(): HealthSignalListener = {
    release()
    this
  }

  override def processWindows(signalPatternMatchers: Seq[SignalPatternMatcher], monitoringActor: Option[ActorRef]): StreamHandle = {
    val windowActors: Seq[HealthSignalWindowActorRef] = {
      val advancerConfig = windowingConfig.advancerConfig
      windowingConfig.frequencies
        .map(freq => HealthSignalWindowActor(actorSystem, freq, WindowSlider(advancerConfig.advanceAmount, advancerConfig.buffer)))
        .map(ref => ref.start(monitoringActor))
    }

    // todo: consider using an akka source
    val future = Executors
      .newCachedThreadPool()
      .submit(new Callable[Unit] {
        override def call: Unit = {
          while (true) {
            val signal = signals.take()
            windowActors.foreach(a => a.processSignal(signal))
          }
        }
      })

    // Stream Handle
    () => {
      future.cancel(true)
      windowActors.foreach(actor => actor.stop())
    }
  }

  override def release(): Unit = {
    releaseWindowHandle()
    // revisit - shouldn't creator stop this actor?
    underlyingActor ! surge.internal.health.windows.stream.actor.Stop
  }

  protected[health] def releaseWindowHandle(): Unit = {
    Option(this.windowHandle).foreach(h => h.release())
  }
}
