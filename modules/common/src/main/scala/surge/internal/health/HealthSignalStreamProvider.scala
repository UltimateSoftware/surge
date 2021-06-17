// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import akka.actor.{ ActorRef, ActorSystem }
import surge.health.HealthSignalStream
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcher
import surge.health.windows.{ WindowStreamListener, _ }
import surge.internal.health.supervisor.HealthSupervisorActor

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
   *   Window[EVENT]
   * @param data
   *   Seq[EVENT]
   */
  override def windowAdvanced(window: Window, data: Seq[HealthSignal]): Unit = {
    actor ! WindowAdvanced(window, WindowData(data, window.duration))
  }

  /**
   * Forward WindowClosed to provided actor
   * @param window
   *   Window[EVENT]
   * @param data
   *   Seq[EVENT]
   */
  override def windowClosed(window: Window, data: Seq[HealthSignal]): Unit = {
    actor ! WindowClosed(window, WindowData(data, window.duration))
  }

  /**
   * Forward WindowOpened to provided actor
   * @param window
   *   Window[EVENT]
   */
  override def windowOpened(window: Window): Unit = {
    actor ! WindowOpened(window)
  }

  /**
   * Forward WindowStopped to provided actor
   * @param window
   *   Window[EVENT]
   */
  override def windowStopped(window: Option[Window]): Unit = {
    actor ! WindowStopped(window)
  }

  /**
   * Forward AddedToWindow to provided actor
   * @param data
   *   EVENT
   * @param window
   *   Window[EVENT]
   */
  override def dataAddedToWindow(data: HealthSignal, window: Window): Unit = {
    actor ! AddedToWindow(data, window)
  }
}

trait HealthSignalStreamProvider {
  private var signalBus: HealthSignalBusInternal = _

  def actorSystem: ActorSystem
  def provide(bus: HealthSignalBusInternal): HealthSignalStream
  def filters(): Seq[SignalPatternMatcher]

  def streamMonitoring: Option[StreamMonitoringRef] = None

  /**
   * Creates a HealthSignalBus with a backing HealthSupervisorActor that is bound to the provided HealthSignalStream. If a signalBus already exists; the
   * existing signalBus is returned.
   * @param startStreamOnInit
   *   Boolean
   * @return
   *   HealthSignalBus
   */
  def busWithSupervision(startStreamOnInit: Boolean = false): HealthSignalBusInternal = {
    Option(signalBus) match {
      case Some(bus) => bus
      case None =>
        signalBus = HealthSignalBus(this, startStreamOnInit).withStreamSupervision(bus => HealthSupervisorActor(bus, filters(), actorSystem), streamMonitoring)

        signalBus
    }
  }
}
