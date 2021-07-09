// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.supervisor

import java.util.regex.Pattern

import akka.Done
import akka.actor.{ Actor, ActorContext, ActorRef, ActorSystem, PoisonPill, Props, Terminated }
import akka.pattern.{ ask, BackoffOpts, BackoffSupervisor }
import akka.util.Timeout
import org.slf4j.{ Logger, LoggerFactory }
import surge.core.{ ControlAck, Controllable }
import surge.health._
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcher
import surge.internal.config.{ BackoffConfig, TimeoutConfig }
import surge.internal.health._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.languageFeature.postfixOps
import scala.util.{ Failure, Success, Try }

trait RegistrationSupport {
  def registrar(): ActorRef
  def register(registration: HealthRegistration): Unit
}

object HealthSignalStreamMonitoringRefWithSupervisionSupport {
  def from(streamMonitoringRef: StreamMonitoringRef): HealthSignalStreamMonitoringRefWithSupervisionSupport = {
    new HealthSignalStreamMonitoringRefWithSupervisionSupport(streamMonitoringRef.actor)
  }
}

class HealthSignalStreamMonitoringRefWithSupervisionSupport(override val actor: ActorRef) extends StreamMonitoringRef(actor) with SupervisionMonitorSupport {

  override def healthSignalReceived(received: HealthSignalReceived): Unit = {
    actor ! received
  }

  override def registrationReceived(received: HealthRegistrationReceived): Unit = {
    actor ! received
  }
}

object HealthSupervisorActorRef {
  val log: Logger = LoggerFactory.getLogger(getClass)
}

/**
 * HealthSupervisorActorRef
 * @param actor
 *   ActorRef
 * @param askTimeout
 *   FiniteDuration
 * @param actorSystem
 *   ActorSystem
 */
class HealthSupervisorActorRef(val actor: ActorRef, askTimeout: FiniteDuration, override val actorSystem: ActorSystem) extends HealthSupervisorTrait {
  import HealthSupervisorActorRef._

  private var started: Boolean = false
  private implicit val executionContext: ExecutionContext = actorSystem.dispatcher
  private val controlled: mutable.Map[String, Controllable] = mutable.Map[String, Controllable]()

  private val controlProxy = actorSystem.actorOf(Props(new Actor() {
    override def receive: Receive = {
      case RestartComponent(name, _) =>
        controlled.get(name) match {
          case Some(controllable) =>
            controllable.restart().andThen { restartControllableCallback(componentName = name, replyTo = sender()) }
          case None =>
            sender() ! ControlAck(success = false, error = Some(new RuntimeException(s"Cannot restart unregistered component $name")))
        }
      case ShutdownComponent(name, _) =>
        controlled.get(name) match {
          case Some(controllable) =>
            val futureResult = controllable.shutdown().andThen { shutdownControllableCallback(componentName = name, replyTo = sender()) }
            sender() ! futureResult
          case None =>
            sender() ! ControlAck(success = false, error = Some(new RuntimeException(s"Cannot shutdown unregistered component $name")))
        }
    }
  }))

  override def registrationLinks(): Seq[HealthRegistrationLink] = {
    controlled.keys.map(name => HealthRegistrationLink(name, ControlProxy(name, controlProxy))).toSeq
  }

  def start(replyTo: Option[ActorRef] = None): HealthSupervisorActorRef = {
    actor ! Start(replyTo)
    started = true
    this
  }

  override def stop(): HealthSupervisorTrait = {
    actor ! Stop
    started = false
    this
  }

  def terminate(): Unit = {
    started = false
    actor ! PoisonPill
  }

  override def state(): HealthSupervisorState = {
    HealthSupervisorState(started)
  }

  override def registrar(): ActorRef = actor

  override def register(registration: HealthRegistration): Future[Any] = {
    val result = actor
      .ask(
        RegisterSupervisedComponentRequest(
          registration.componentName,
          controlProxy,
          restartSignalPatterns = registration.restartSignalPatterns,
          shutdownSignalPatterns = registration.shutdownSignalPatterns))(askTimeout)
      .andThen {
        case Success(_) =>
          controlled.put(registration.componentName, registration.control)
        case Failure(exception) =>
          log.error(s"Failed to register ${registration.componentName}", exception)
      }

    result
  }

  private def restartControllableCallback(componentName: String, replyTo: ActorRef): PartialFunction[Try[ControlAck], Unit] = {
    case Failure(exception) =>
      log.error(s"$componentName failed to restart", exception)
      replyTo ! ControlAck(success = false, error = Some(exception))
    case Success(ack) =>
      if (ack.success) {
        log.debug(s"$componentName was restarted successfully")
        replyTo ! ControlAck(success = true)
      } else {
        log.error(s"$componentName failed to restart", ack.error.orNull)
        replyTo ! ControlAck(success = false, error = None)
      }
  }
  private def shutdownControllableCallback(componentName: String, replyTo: ActorRef): PartialFunction[Try[ControlAck], Unit] = {
    case Failure(exception) =>
      log.error(s"$componentName failed to shutdown", exception)
    case Success(ack) =>
      if (ack.success) {
        log.debug(s"$componentName was shutdown successfully")
        // remove control
        controlled.remove(componentName)
        actor ! UnregisterSupervisedComponentRequest(componentName)
        replyTo ! ControlAck(success = true)
      } else {
        log.error(s"$componentName failed to shutdown", ack.error.orNull)
        replyTo ! ControlAck(success = false, error = Some(new RuntimeException(s"$componentName failed to shutdown", ack.error.orNull)))
      }
  }
}

// Commands
case class Start(replyTo: Option[ActorRef] = None)
case class RestartComponent(name: String, replyTo: ActorRef)
case class ShutdownComponent(name: String, replyTo: ActorRef)
case class UnregisterSupervisedComponentRequest(componentName: String)
case class RegisterSupervisedComponentRequest(
    componentName: String,
    controlProxyRef: ActorRef,
    restartSignalPatterns: Seq[Pattern],
    shutdownSignalPatterns: Seq[Pattern]) {
  def asSupervisedComponentRegistration(): SupervisedComponentRegistration =
    SupervisedComponentRegistration(componentName, controlProxyRef, restartSignalPatterns, shutdownSignalPatterns)
}
case class HealthRegistrationDetailsRequest()
case class Stop()

// State
case class SupervisedComponentRegistration(
    componentName: String,
    controlProxyRef: ActorRef,
    restartSignalPatterns: Seq[Pattern],
    shutdownSignalPatterns: Seq[Pattern])
case class HealthState(registered: Map[String, SupervisedComponentRegistration] = Map.empty, replyTo: Option[ActorRef] = None)

object HealthSupervisorActor {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def apply(signalBus: HealthSignalBusInternal, filters: Seq[SignalPatternMatcher], actorSystem: ActorSystem): HealthSupervisorActorRef = {
    val props = BackoffSupervisor.props(
      BackoffOpts
        .onFailure(
          Props(new HealthSupervisorActor(signalBus, filters)),
          childName = "healthSupervisorActor",
          minBackoff = BackoffConfig.HealthSupervisorActor.minBackoff,
          maxBackoff = BackoffConfig.HealthSupervisorActor.maxBackoff,
          randomFactor = BackoffConfig.HealthSupervisorActor.randomFactor)
        .withMaxNrOfRetries(BackoffConfig.HealthSupervisorActor.maxRetries))
    val actorRef = actorSystem.actorOf(props)
    new HealthSupervisorActorRef(actorRef, 30.seconds, actorSystem)
  }

  protected[supervisor] class ContextForwardingRegistrationHandler(context: ActorContext) extends RegistrationHandler {
    override def handle(registration: HealthRegistration): Try[Done] = Try {
      context.self ! registration
      Done
    }
  }

  protected[supervisor] class ContextForwardingSignalHandler(context: ActorContext) extends SignalHandler {
    override def handle(signal: HealthSignal): Try[Done] = Try {
      context.self ! signal
      Done
    }
  }
}

class HealthSupervisorActor(internalSignalBus: HealthSignalBusInternal, filters: Seq[SignalPatternMatcher])
    extends Actor
    with HealthSignalListener
    with HealthRegistrationListener {
  import HealthSupervisorActor._

  implicit val askTimeout: Timeout = TimeoutConfig.HealthSupervision.actorAskTimeout
  implicit val executionContext: ExecutionContext = ExecutionContext.global

  val state: HealthState = HealthState()

  val signalHandler: SignalHandler = new ContextForwardingSignalHandler(context)
  val registrationHandler: RegistrationHandler = new ContextForwardingRegistrationHandler(context)

  override def id(): String = "HealthSuperVisorActor_1"

  override def receive: Receive = {
    case Start(replyTo) =>
      start(maybeSideEffect = Some(() => {
        context.become(monitoring(state.copy(replyTo = replyTo)))
      }))
    case Stop =>
      stop()
  }

  override def start(maybeSideEffect: Option[() => Unit]): HealthSignalListener = {
    this.subscribe(signalHandler)
    this.listen(registrationHandler)

    maybeSideEffect.foreach(m => m())
    this
  }

  override def stop(): HealthSignalListener = {
    unsubscribe()
    context.stop(self)
    this
  }

  override def signalBus(): HealthSignalBusInternal = internalSignalBus

  override def subscribe(signalHandler: SignalHandler): HealthSignalListener = {
    super.bindSignalHandler(signalHandler)
    signalBus().subscribe(subscriber = this, signalTopic)
    this
  }

  override def listen(registrationHandler: RegistrationHandler): HealthRegistrationListener = {
    super.bindRegistrationHandler(registrationHandler)
    signalBus().subscribe(subscriber = this, signalBus().registrationTopic())
    this
  }

  override def handleMessage(message: HealthMessage): Unit = {
    message match {
      case reg: HealthRegistration =>
        handleRegistration(reg)
      case sig: HealthSignal =>
        handleSignal(sig)
      case other =>
        log.error(s"Unable to handle message of type $other.getClass()")
    }
  }

  def monitoring(state: HealthState): Receive = {
    case Stop =>
      context.become(receive)
      context.self ! Stop
    case reg: RegisterSupervisedComponentRequest =>
      state.replyTo.foreach(r => r ! HealthRegistrationReceived(reg))
      context.watch(reg.controlProxyRef)
      context.become(monitoring(state.copy(registered = state.registered + (reg.componentName -> reg.asSupervisedComponentRegistration()))))
      sender() ! Ack(success = true, None)
    case remove: UnregisterSupervisedComponentRequest =>
      context.become(monitoring(state.copy(registered = state.registered - remove.componentName)))
    case HealthRegistrationDetailsRequest =>
      sender() ! state.registered.values.toList
    case signal: HealthSignal =>
      state.replyTo.foreach(r => r ! HealthSignalReceived(signal))

      state.registered.values.foreach(registered => {
        // Restart
        processRestart(signal, registered, state)
        // Shutdown
        processShutdown(signal, registered, state)
      })
    case term: Terminated =>
      context.unwatch(term.actor)
      val nextState = state.copy(registered = Map.empty)
      context.become(monitoring(nextState))
  }

  // Private
  private def processShutdown(signal: HealthSignal, registered: SupervisedComponentRegistration, state: HealthState): Unit = {
    registered.shutdownSignalPatterns.foreach(p => {
      if (p.matcher(signal.name).matches()) {
        attemptShutdown(registered).onComplete {
          case Failure(err) =>
            val event = ShutdownComponentFailed(registered.componentName, error = Some(err))
            state.replyTo.foreach(r => r ! event)
          case Success(events) =>
            state.replyTo.foreach(r => events.foreach(e => r ! e))
        }
      }
    })
  }

  private def processRestart(signal: HealthSignal, registered: SupervisedComponentRegistration, state: HealthState): Unit = {
    registered.restartSignalPatterns.foreach(p => {
      if (p.matcher(signal.name).matches()) {
        attemptRestart(registered).onComplete {
          case Failure(err) =>
            val events = Set(RestartComponentAttempted(registered.componentName), RestartComponentFailed(registered.componentName, error = Some(err)))
            state.replyTo.foreach(r => events.foreach(e => r ! e))
          case Success(events) =>
            state.replyTo.foreach(r =>
              events.foreach(e => {
                log.debug("replying with event", e)
                r ! e
              }))
        }
      }
    })
  }

  private def attemptRestart(registered: SupervisedComponentRegistration): Future[Set[HealthSupervisionEvent]] = {
    registered.controlProxyRef.ask(RestartComponent(registered.componentName, self)).map[Set[HealthSupervisionEvent]] {
      case ack: ControlAck =>
        if (ack.success) {
          Set(RestartComponentAttempted(registered.componentName), ComponentRestarted(registered.componentName))
        } else {
          Set(RestartComponentAttempted(registered.componentName), RestartComponentFailed(registered.componentName, error = ack.error))
        }
      case other =>
        Set(
          RestartComponentAttempted(registered.componentName),
          RestartComponentFailed(
            registered.componentName,
            error = Some(new RuntimeException(s"Unknown response received from RestartComponent request ${other.getClass}"))))
    }
  }

  private def attemptShutdown(registered: SupervisedComponentRegistration): Future[Set[HealthSupervisionEvent]] = {
    registered.controlProxyRef.ask(ShutdownComponent(registered.componentName, self)).map[Set[HealthSupervisionEvent]] { case ack: ControlAck =>
      if (ack.success) {
        Set(ShutdownComponentAttempted(registered.componentName), ComponentShutdown(registered.componentName))
      } else {
        Set(ShutdownComponentAttempted(registered.componentName), ShutdownComponentFailed(registered.componentName, error = ack.error))
      }
    }
  }
}
