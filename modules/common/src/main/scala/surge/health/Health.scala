// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health

import java.time.Instant
import java.util.UUID
import java.util.regex.Pattern

import akka.Done
import akka.actor.ActorRef
import akka.event.EventBus
import surge.core.Controllable
import surge.health.domain.{ EmittableHealthSignal, Error, HealthSignal, Timed, Trace, Warning }
import surge.health.matchers.SignalPatternMatcher
import surge.internal.health.RegistrationHandler

import scala.concurrent.Future
import scala.util.Try

trait InvokableHealthRegistration {
  def invoke(): InvokableHealthRegistration

  def underlyingRegistration(): HealthRegistration
}

trait HealthSignalBusAware {
  def signalBus(): HealthSignalBusTrait
}

trait HealthyPublisher extends HealthSignalBusAware {
  def signalMetadata(): Map[String, String] = Map.empty
}

trait RegistrationConsumer {
  def registrations(): Future[Seq[HealthRegistration]]
}

trait ControlReference {
  def componentName(): String
  def ref(): Controllable
}

final case class HealthRegistration(
    ref: ActorRef,
    topic: String,
    name: String,
    restartSignalPatterns: Seq[Pattern] = Seq.empty,
    shutdownSignalPatterns: Seq[Pattern] = Seq.empty,
    id: UUID = UUID.randomUUID(),
    timestamp: Instant = Instant.now)
    extends HealthMessage
trait RegistrationProducer {
  def register(ref: ActorRef, componentName: String, restartSignalPatterns: Seq[Pattern], shutdownSignalPatterns: Seq[Pattern] = Seq.empty): Unit
  def registration(
      ref: ActorRef,
      componentName: String,
      restartSignalPatterns: Seq[Pattern],
      shutdownSignalPatterns: Seq[Pattern] = Seq.empty): InvokableHealthRegistration
}

trait HealthMessage extends Timed {
  def topic(): String
  def id(): UUID
  def timestamp: Instant
}

trait BusSupervisionTrait extends RegistrationProducer with RegistrationConsumer

trait HealthRegistrationListener extends HealthListener {
  private var handler: RegistrationHandler = _
  def signalBus(): HealthSignalBusTrait
  def id(): String

  def registrationTopic(): String = signalBus().registrationTopic()
  def bindRegistrationHandler(handler: RegistrationHandler): Unit = {
    this.handler = handler
  }

  def handleRegistration(registration: HealthRegistration): Unit = {
    Option(this.handler).foreach(h => h.handle(registration))
  }

  def listen(registrationHandler: RegistrationHandler): HealthRegistrationListener

  override def compareTo(o: String): Int = {
    o.compareTo(this.id())
  }

  override def handleMessage(message: HealthMessage): Unit = {
    message match {
      case registration: HealthRegistration =>
        handleRegistration(registration)
      case other =>
        log.error(s"Unable to handle message of type ${other.getClass}")
    }
  }

  def ignoreRegistrations(): HealthRegistrationListener = {
    signalBus().unsubscribe(subscriber = this)
    this
  }

}

case class HealthSupervisorState(started: Boolean)

trait HealthSupervisorTrait {
  def state(): HealthSupervisorState
  def stop(): HealthSupervisorTrait
  def start(replyTo: Option[ActorRef] = None): HealthSupervisorTrait

  def register(registration: HealthRegistration): HealthSupervisorTrait

  def registrar(): ActorRef
}

trait SignalProducer {
  def signalWithError(name: String, error: Error, metadata: Map[String, String] = Map.empty): EmittableHealthSignal
  def signalWithWarning(name: String, warning: Warning, metadata: Map[String, String] = Map.empty): EmittableHealthSignal
  def signalWithTrace(name: String, trace: Trace, metadata: Map[String, String] = Map.empty): EmittableHealthSignal
}

trait HealthSignalBusTrait extends EventBus with BusSupervisionTrait with SignalProducer {
  type Event = HealthMessage
  type Classifier = String
  type Subscriber = HealthListener

  def supervise(): HealthSignalBusTrait

  def unsupervise(): HealthSignalBusTrait

  def registrationTopic(): String
  def signalTopic(): String

  def supervisor(): Option[HealthSupervisorTrait]

  def signalStream(): HealthSignalStream
}

trait HealthListener extends Comparable[String] {
  def id(): String

  def handleMessage(message: HealthMessage): Unit

  override def toString: String = id()
}

trait HealthSignalListener extends HealthListener {
  private var handler: SignalHandler = _
  def signalBus(): HealthSignalBusTrait

  def signalTopic: String = signalBus().signalTopic()
  def bindSignalHandler(handler: SignalHandler): Unit = {
    this.handler = handler
  }

  def handleSignal(signal: HealthSignal): Unit = {
    Option(this.handler).foreach(h => h.handle(signal))
  }

  override def compareTo(o: String): Int = {
    o.compareTo(this.id())
  }

  override def handleMessage(message: HealthMessage): Unit = {
    message match {
      case signal: HealthSignal =>
        handleSignal(signal)
      case other =>
        log.error(s"Unable to handle message of type ${other.getClass}")
    }
  }

  def subscribeWithFilters(signalHandler: SignalHandler, filters: Seq[SignalPatternMatcher] = Seq.empty): HealthSignalListener

  def unsubscribe(): HealthSignalListener = {
    signalBus().unsubscribe(subscriber = this)
    this
  }

  def start(maybeSideEffect: Option[() => Unit] = None): HealthSignalListener
  def stop(): HealthSignalListener
}

trait SignalHandler {
  def handle(signal: HealthSignal): Try[Done]
}

trait HealthSignalStream extends HealthSignalListener {
  def signalTopic: String
  def signalHandler: SignalHandler

  def filters(): Seq[SignalPatternMatcher]
  def subscribe(): HealthSignalStream = {
    subscribeWithFilters(signalHandler, filters()).asInstanceOf[HealthSignalStream]
  }
}
