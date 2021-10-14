// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import surge.health.config.HealthSignalBusConfig
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcher
import surge.health.windows.{ WindowStreamListener, _ }
import surge.health.{ HealthSignalBusTrait, HealthSignalListener, HealthSignalStream, SignalHandler }
import surge.internal.health.supervisor.HealthSupervisorActor

import scala.util.Success

/**
 * StreamMonitoringRef holds a reference to an Actor responsible for forwarding stream processing events to an underlying akka Actor via the provided ActorRef.
 *
 * @param actor
 *   ActorRef
 */
class StreamMonitoringRef(override val actor: ActorRef) extends WindowStreamListener with HealthSupervisionMonitoring {

  /**
   * Forward WindowAdvanced to provided actor
   * @param window
   *   Window
   * @param data
   *   Seq[HealthSignal]
   */
  override def windowAdvanced(window: Window, data: Seq[HealthSignal]): Unit = {
    actor ! WindowAdvanced(window, WindowData(data, window.duration))
  }

  /**
   * Forward WindowClosed to provided actor
   * @param window
   *   Window
   * @param data
   *   Seq[HealthSignal]
   */
  override def windowClosed(window: Window, data: Seq[HealthSignal]): Unit = {
    actor ! WindowClosed(window, WindowData(data, window.duration))
  }

  /**
   * Forward WindowOpened to provided actor
   * @param window
   *   Window
   */
  override def windowOpened(window: Window): Unit = {
    actor ! WindowOpened(window)
  }

  /**
   * Forward WindowStopped to provided actor
   * @param window
   *   Window
   */
  override def windowStopped(window: Option[Window]): Unit = {
    actor ! WindowStopped(window)
  }

  /**
   * Forward AddedToWindow to provided actor
   * @param data
   *   HealthSignal
   * @param window
   *   Window
   */
  override def dataAddedToWindow(data: HealthSignal, window: Window): Unit = {
    actor ! AddedToWindow(data, window)
  }
}

trait HealthSignalStreamProvider {
  private val signalBus: HealthSignalBusInternal =
    HealthSignalBus(signalStream = this).withStreamSupervision(bus => HealthSupervisorActor(bus, filters(), actorSystem), streamMonitoring)

  def actorSystem: ActorSystem
  def provide(bus: HealthSignalBusInternal): HealthSignalStream
  def filters(): Seq[SignalPatternMatcher]

  def streamMonitoring: Option[StreamMonitoringRef] = None

  /**
   * Creates a HealthSignalBus with a backing HealthSupervisorActor that is bound to the provided HealthSignalStream. If a signalBus already exists; the
   * existing signalBus is returned.
   * @return
   *   HealthSignalBus
   */
  def bus(): HealthSignalBusInternal = signalBus
}

class NullHealthSignalStream(bus: HealthSignalBusTrait, override val actorSystem: ActorSystem) extends HealthSignalStream {

  override def signalHandler: SignalHandler = (_: HealthSignal) => Success(Done)

  override def filters(): Seq[SignalPatternMatcher] = Seq.empty

  override def signalBus(): HealthSignalBusTrait = bus

  // no need to subscribe
  override def subscribe(signalHandler: SignalHandler): HealthSignalListener = this

  override def start(maybeSideEffect: Option[() => Unit]): HealthSignalListener = this

  override def stop(): HealthSignalListener = this

  override def id(): String = "null-health-signal-stream"
}

class DisabledHealthSignalStreamProvider(bus: HealthSignalBusTrait, override val actorSystem: ActorSystem) extends HealthSignalStreamProvider {
  private lazy val nullStream: HealthSignalStream = new NullHealthSignalStream(bus, actorSystem)

  override def provide(bus: HealthSignalBusInternal): HealthSignalStream = nullStream

  override def filters(): Seq[SignalPatternMatcher] = Seq.empty
}
