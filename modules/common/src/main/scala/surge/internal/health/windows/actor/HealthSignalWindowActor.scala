// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.actor

import java.time.Instant

import akka.actor.{ Actor, ActorRef, ActorSystem, Cancellable, PoisonPill, Props, Stash }
import akka.pattern.{ BackoffOpts, BackoffSupervisor }
import org.slf4j.{ Logger, LoggerFactory }
import surge.health.domain.HealthSignal
import surge.health.windows.{ AddedToWindow, Advancer, Window, WindowAdvanced, WindowClosed, WindowData, WindowOpened, WindowStopped }
import surge.internal.config.BackoffConfig
import surge.internal.health.windows._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.languageFeature.postfixOps

case class WindowState(window: Option[Window] = None, replyTo: Option[ActorRef] = None)

object HealthSignalWindowActor {
  val log: Logger = LoggerFactory.getLogger(getClass)

  case class Start(window: Window, replyTo: ActorRef)
  case class Tick()

  case class Stop()

  def apply(
      actorSystem: ActorSystem,
      initialWindowProcessingDelay: FiniteDuration,
      windowFrequency: FiniteDuration,
      advancer: Advancer[Window],
      windowCheckInterval: FiniteDuration = 1.second): HealthSignalWindowActorRef = {

    // note: we lose the window data on restarts
    val props = BackoffSupervisor.props(
      BackoffOpts
        .onFailure(
          Props(new HealthSignalWindowActor(windowFrequency, advancer)),
          childName = "healthSignalWindowActor",
          minBackoff = BackoffConfig.HealthSignalWindowActor.minBackoff,
          maxBackoff = BackoffConfig.HealthSignalWindowActor.maxBackoff,
          randomFactor = BackoffConfig.HealthSignalWindowActor.randomFactor)
        .withMaxNrOfRetries(BackoffConfig.HealthSignalWindowActor.maxRetries))

    val windowActor = actorSystem.actorOf(props)
    new HealthSignalWindowActorRef(windowActor, initialWindowProcessingDelay, windowFrequency, actorSystem, windowCheckInterval)
  }
}

class HealthSignalWindowActorRef(
    val actor: ActorRef,
    initialWindowProcessingDelay: FiniteDuration,
    windowFreq: FiniteDuration,
    actorSystem: ActorSystem,
    windowCheckInterval: FiniteDuration = 1.second) {
  import HealthSignalWindowActor._

  private var listener: WindowStreamListeningActorRef = _

  private val scheduledTask: Cancellable =
    actorSystem.scheduler.scheduleAtFixedRate(initialDelay = initialWindowProcessingDelay, interval = windowCheckInterval)(() => actor ! Tick())(
      ExecutionContext.global)

  def start(replyTo: Option[ActorRef]): HealthSignalWindowActorRef = {
    val window: Window = Window.windowFor(Instant.now(), windowFreq)
    // Listener of Window Streaming events generated by the HealthSignalWindowActor
    listener = WindowStreamListeningActorAdapter(actorSystem, replyTo)
    actor ! Start(window, listener.actor)
    this
  }

  def tick(): HealthSignalWindowActorRef = {
    actor ! Tick()
    this
  }

  def stop(): HealthSignalWindowActorRef = {
    actor ! Stop()
    scheduledTask.cancel()
    this
  }

  def closeWindow(): HealthSignalWindowActorRef = {
    actor ! CloseCurrentWindow()
    this
  }

  def terminate(): Unit = {
    actor ! PoisonPill
  }

  def processSignal(signal: HealthSignal): HealthSignalWindowActorRef = {
    actor ! signal
    this
  }
}

/**
 * HealthSignalWindowActor is responsible for managing a Window of HealthSignal data and determining how and when to advance the window using a provided
 * windowAdvanceStrategy Advancer.
 *
 * All HealthSignal(s) received when the Window has been opened via OpenWindow command; are appended to the Window. When a Window is closed via CloseWindow
 * command; HealthSignal(s) that have accumulated in the Window are delivered in a WindowClosed event. When a Window is advanced via AdvanceWindow command;
 * HealthSignal(s) that have accumulated in the Window are delivered in a WindowAdvanced event.
 *
 * @param frequency
 *   FiniteDuration
 * @param windowAdvanceStrategy
 *   Advancer
 */
class HealthSignalWindowActor(frequency: FiniteDuration, windowAdvanceStrategy: Advancer[Window]) extends Actor with Stash {
  import HealthSignalWindowActor._

  /**
   * Initialization
   * @return
   *   Receive
   */
  override def receive: Receive = {
    case Start(window, replyTo) =>
      log.trace("Starting window {}", window)
      unstashAll()
      context.self ! OpenWindow(window, None)
      context.become(ready(WindowState().copy(replyTo = Some(replyTo))))
    case Stop() =>
      context.stop(self)
    case HealthSignal => stash()
  }

  /**
   * Ready to handle a Window
   * @param state
   *   WindowState
   * @return
   *   Receive
   */
  def ready(state: WindowState): Receive = {
    case AdvanceWindow(w, n) =>
      reportWindowAdvanced(w, n, state)
      context.self ! OpenWindow(n, None)
      context.become(ready(state), discardOld = true)
    case OpenWindow(w, maybeSignal) =>
      context.become(handleOpenWindow(w, maybeSignal, state), discardOld = true)
    case Stop() => handleStop(state)

    case _ => stash()
  }

  /**
   * Processing active Window
   * @param state
   *   WindowState
   * @return
   *   Receive
   */
  def windowing(state: WindowState): Receive = {
    case signal: HealthSignal =>
      context.become(handleSignal(signal, state))

    case AdvanceWindow(w, n) =>
      context.become(handleAdvanceWindow(w, n, state))
    case AddToWindow(signal: HealthSignal, window) =>
      context.become(handleAddToWindow(window, signal, state))
    case CloseWindow(window, advance) =>
      context.become(handleCloseWindow(window, advance, state))
    case Stop() => handleStop(state)
    case Tick() => handleTick(state)

    case CloseCurrentWindow =>
      state.window.foreach(w => context.self ! CloseWindow(w, advance = true))
  }

  private def handleStop(state: WindowState): Unit = {
    state.replyTo.foreach(a => {
      log.trace("Notifying {} that windowing stopped", a)
      a ! WindowStopped(state.window)
    })

    state.window.foreach { w =>
      state.replyTo.foreach(a => {
        log.trace("Notifying {} that window closed", a)
        a ! WindowClosed(w, WindowData(state.window.map(w => w.data).getOrElse(Seq.empty), frequency))
      })
    }
    context.stop(self)
  }

  private def handleSignal(signal: HealthSignal, state: WindowState): Receive = {
    state.window.foreach(w => context.self ! AddToWindow(signal, w))
    windowing(state)
  }

  private def handleAddToWindow(window: Window, signal: HealthSignal, state: WindowState): Receive = {
    advanceWindowCommand(window.copy(data = window.data ++ Seq(signal))).foreach(cmd => {
      context.self ! cmd
    })
    state.replyTo.foreach(r => {
      log.trace("Notifying {} that Health Signal was added to window", r)
      r ! AddedToWindow(signal, window)
    })

    windowing(state.copy(window = Some(window.copy(data = state.window.map(w => w.data ++ Seq[HealthSignal](signal)).getOrElse(Seq.empty)))))
  }

  private def handleAdvanceWindow(window: Window, newWindow: Window, state: WindowState): Receive = {
    log.trace("Window advanced {}", window)
    reportWindowAdvanced(window, newWindow, state)
    windowing(state = state.copy(window = Some(newWindow)))
  }

  private def handleCloseWindow(window: Window, advance: Boolean, state: WindowState): Receive = {
    val capturedSignals = state.window.map(w => w.data).getOrElse(Seq.empty)
    log.trace("Closing window {} and informing {}", window, state.replyTo)
    state.replyTo.foreach(r => {
      log.trace("Notifying {} that window closed", r)
      r ! WindowClosed(window, WindowData(capturedSignals, frequency))
    })
    val nextBehavior = ready(state.copy(window = None))

    if (advance) {
      // Advance Window on Close
      log.trace("Checking if advance should occur")
      advanceWindowCommand(window, force = true).foreach(cmd => context.self ! cmd)
    }

    nextBehavior
  }

  protected def handleTick(state: WindowState): Unit = {
    state.window.foreach(w => {
      if (w.expired()) {
        context.self ! CloseWindow(w, advance = true)
      }
    })
  }

  private def handleOpenWindow(window: Window, maybeSignal: Option[HealthSignal], state: WindowState): Receive = {
    val nextState = state.copy(window = Some(window.copy()))

    unstashAll()

    maybeSignal.foreach(s => context.self ! s)

    log.trace("Window opened {}", window)
    state.replyTo.foreach(r => {
      log.trace("Notifying {} that window opened", r)
      r ! WindowOpened(window)
    })

    windowing(nextState)
  }

  private def advanceWindowCommand(window: Window, force: Boolean = false): Option[AdvanceWindow] = {
    val maybeAdvanced: Option[Window] = windowAdvanceStrategy.advance(window, force)
    log.trace("Possible Window Advance => {}", maybeAdvanced)
    maybeAdvanced.map(next => AdvanceWindow(window, next))
  }

  private def reportWindowAdvanced(oldWindow: Window, newWindow: Window, state: WindowState): Unit = {
    state.replyTo.foreach(r => {
      log.trace("Notifying {} that window has advanced", r)
      r ! WindowAdvanced(newWindow, WindowData(oldWindow.data, frequency))
    })
  }
}
