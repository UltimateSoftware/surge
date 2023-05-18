// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.health.supervisor

import akka.Done
import akka.actor.{ Actor, ActorContext, ActorRef, ActorSystem, PoisonPill, Props, Terminated }
import akka.pattern.{ ask, BackoffOpts, BackoffSupervisor }
import akka.util.Timeout
import org.slf4j.{ Logger, LoggerFactory }
import surge.core.{ Ack, Controllable, ControllableLookup, ControllableRemover }
import surge.health._
import surge.health.config.HealthSupervisorConfig
import surge.health.domain.HealthSignal
import surge.health.jmx.Api.{ AddComponent, RemoveComponent, StartManagement, StopManagement }
import surge.health.jmx.Domain.HealthRegistrationDetail
import surge.health.jmx.{ HealthJmxTrait, SurgeHealthActor }
import surge.health.supervisor.Api._
import surge.health.supervisor.Domain.SupervisedComponentRegistration
import surge.internal.config.{ BackoffConfig, TimeoutConfig }
import surge.internal.health._
import surge.jmx.ActorJMXSupervisor

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

private class ControllableLookupImpl(resource: mutable.Map[String, Controllable]) extends ControllableLookup {
  override def lookup(identifier: String): Option[Controllable] = {
    resource.get(identifier)
  }
}

private class ControllableRemoverImpl(resource: mutable.Map[String, Controllable]) extends ControllableRemover {
  override def remove(identifier: String): Option[Controllable] = {
    resource.remove(identifier)
  }
}

class ControlProxyActor(finder: ControllableLookup, remover: ControllableRemover, supervisorActorRef: ActorRef, actorSystem: ActorSystem)(
    implicit ec: ExecutionContext)
    extends Actor {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val askTimeout: Timeout = TimeoutConfig.HealthSupervision.actorAskTimeout

  override def receive: Receive = {
    case QueryComponentExists(name) =>
      sender ! finder.lookup(name).isDefined
    case RestartComponent(name, _) =>
      finder.lookup(name) match {
        case Some(controllable) =>
          log.debug(s"Restarting component $name")
          controllable.restart().andThen { restartControllableCallback(componentName = name, replyTo = sender()) }
        case None =>
          sender() ! akka.actor.Status.Failure(new RuntimeException(s"Cannot restart unregistered component $name"))
      }
    case ShutdownComponent(name, _) =>
      finder.lookup(name) match {
        case Some(controllable) =>
          controllable.shutdown().andThen { shutdownControllableCallback(componentName = name, replyTo = sender()) }
        case None =>
          sender() ! akka.actor.Status.Failure(new RuntimeException(s"Cannot shutdown unregistered component $name"))
      }
  }

  private def restartControllableCallback(componentName: String, replyTo: ActorRef): PartialFunction[Try[Ack], Unit] = {
    case Failure(exception) =>
      log.error(s"$componentName failed to restart", exception)
      replyTo ! akka.actor.Status.Failure(exception)
    case Success(ack) =>
      log.debug(s"$componentName was restarted successfully")
      replyTo ! ack
  }

  private def shutdownControllableCallback(componentName: String, replyTo: ActorRef): PartialFunction[Try[Ack], Unit] = {
    case Failure(exception) =>
      log.error(s"$componentName failed to shutdown", exception)
    case Success(_) =>
      log.debug(s"$componentName was shutdown successfully")
      supervisorActorRef.ask(UnregisterSupervisedComponentRequest(componentName)).andThen {
        case Failure(exception) =>
          replyTo ! akka.actor.Status.Failure(exception)
        case Success(ack: Ack) =>
          remover.remove(componentName)
          replyTo ! ack
      }
  }
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
  private val log: Logger = LoggerFactory.getLogger(getClass)

  private var started: Boolean = false

  private implicit val executionContext: ExecutionContext = actorSystem.dispatcher
  private val controlled: mutable.Map[String, Controllable] = mutable.Map[String, Controllable]()

  private val controlProxy =
    actorSystem.actorOf(Props(new ControlProxyActor(new ControllableLookupImpl(controlled), new ControllableRemoverImpl(controlled), actor, actorSystem)))

  override def registrationLinks(): Seq[HealthRegistrationLink] = {
    controlled.keys.map(name => HealthRegistrationLink(name, ControlProxy(name, controlProxy))).toSeq
  }

  def start(replyTo: Option[ActorRef] = None): HealthSupervisorActorRef = {
    actor ! Start(replyTo)
    started = true
    this
  }

  override def stop(): HealthSupervisorTrait = {
    actor ! Stop()
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

  override def unregister(componentName: String): Future[Ack] = {
    actor
      .ask(UnregisterSupervisedComponentRequest(componentName))(askTimeout)
      .andThen {
        case Success(_) =>
          controlled.remove(componentName)
        case Failure(exception) =>
          log.error(s"Failed to register $componentName", exception)

      }
      .mapTo[Ack]
  }

  override def register(registration: HealthRegistration): Future[Ack] = {
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

    result.mapTo[Ack]
  }
}

// State
case class HealthState(registered: Map[String, SupervisedComponentRegistration] = Map.empty, replyTo: Option[ActorRef] = None)

object HealthSupervisorActor {
  val config: HealthSupervisorConfig = HealthSupervisorConfig()

  def apply(signalBus: HealthSignalBusInternal, actorSystem: ActorSystem, config: HealthSupervisorConfig): HealthSupervisorActorRef = {
    val props = BackoffSupervisor.props(
      BackoffOpts
        .onFailure(
          Props(new HealthSupervisorActor(signalBus, config)),
          childName = "healthSupervisorActor",
          minBackoff = BackoffConfig.HealthSupervisorActor.minBackoff,
          maxBackoff = BackoffConfig.HealthSupervisorActor.maxBackoff,
          randomFactor = BackoffConfig.HealthSupervisorActor.randomFactor)
        .withMaxNrOfRetries(BackoffConfig.HealthSupervisorActor.maxRetries))
    val actorRef = actorSystem.actorOf(props)
    new HealthSupervisorActorRef(actorRef, 30.seconds, actorSystem)
  }

  def apply(signalBus: HealthSignalBusInternal, actorSystem: ActorSystem): HealthSupervisorActorRef = {
    apply(signalBus, actorSystem, config)
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

class HealthSupervisorActor(internalSignalBus: HealthSignalBusInternal, config: HealthSupervisorConfig)
    extends Actor
    with ActorJMXSupervisor
    with HealthSignalListener
    with HealthRegistrationListener
    with HealthJmxTrait {
  implicit val askTimeout: Timeout = TimeoutConfig.HealthSupervision.actorAskTimeout
  implicit val executionContext: ExecutionContext = ExecutionContext.global

  import HealthSupervisorActor._
  val state: HealthState = HealthState()

  private[this] val jmxActor: Option[ActorRef] = if (config.jmxEnabled) {
    Some(context.actorOf(Props(SurgeHealthActor(asActorRef()))))
  } else {
    None
  }

  val signalHandler: SignalHandler = new ContextForwardingSignalHandler(context)
  val registrationHandler: RegistrationHandler = new ContextForwardingRegistrationHandler(context)

  override def id(): String = "HealthSuperVisorActor_1"
  override def asActorRef(): ActorRef = context.self

  override def getJmxActor: Option[ActorRef] = jmxActor

  override def receive: Receive = {
    case Start(replyTo) =>
      start(maybeSideEffect = Some(() => {
        getJmxActor.foreach(a => a ! StartManagement())
        context.become(monitoring(state.copy(replyTo = replyTo)))
      }))
    case Stop() =>
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
    getJmxActor.foreach(a => a ! StopManagement())
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
    case Stop() =>
      context.become(receive)
      context.self ! Stop
    case reg: RegisterSupervisedComponentRequest =>
      state.replyTo.foreach(r => r ! HealthRegistrationReceived(reg))
      context.watch(reg.controlProxyRef)
      context.become(monitoring(state.copy(registered = state.registered + (reg.componentName -> reg.asSupervisedComponentRegistration()))))
      sender() ! Ack
      getJmxActor.foreach(a => a ! AddComponent(HealthRegistrationDetail(reg.componentName, reg.controlProxyRef.path.toStringWithoutAddress)))
    case remove: UnregisterSupervisedComponentRequest =>
      context.become(monitoring(state.copy(registered = state.registered - remove.componentName)))
      sender() ! Ack
      getJmxActor.foreach(a => a ! RemoveComponent(remove.componentName))
    case HealthRegistrationDetailsRequest() =>
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
                log.debug("replying with event {}", e)
                r ! e
              }))
            signal.source.foreach(s => s.flush())
        }
      }
    })
  }

  private def attemptRestart(registered: SupervisedComponentRegistration): Future[Set[HealthSupervisionEvent]] = {
    registered.controlProxyRef.ask(RestartComponent(registered.componentName, self)).map[Set[HealthSupervisionEvent]] {
      case _: Ack =>
        Set(RestartComponentAttempted(registered.componentName), ComponentRestarted(registered.componentName))
      case other =>
        Set(
          RestartComponentAttempted(registered.componentName),
          RestartComponentFailed(
            registered.componentName,
            error = Some(new RuntimeException(s"Unknown response received from RestartComponent request ${other.getClass}"))))
    }
  }

  private def attemptShutdown(registered: SupervisedComponentRegistration): Future[Set[HealthSupervisionEvent]] = {
    registered.controlProxyRef.ask(ShutdownComponent(registered.componentName, self)).map[Set[HealthSupervisionEvent]] {
      case _: Ack =>
        Set(ShutdownComponentAttempted(registered.componentName), ComponentShutdown(registered.componentName))
      case other =>
        Set(
          ShutdownComponentAttempted(registered.componentName),
          ShutdownComponentFailed(
            registered.componentName,
            error = Some(new RuntimeException(s"Unknown response received from RestartComponent request ${other.getClass}"))))
    }
  }
}
