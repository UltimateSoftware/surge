// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health

import akka.actor.{ ActorRef, ActorSystem, NoSerializationVerificationNeeded }
import akka.event.EventBus
import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source, SourceQueueWithComplete }
import akka.{ Done, NotUsed }
import org.slf4j.LoggerFactory
import surge.core.{ Ack, Controllable }
import surge.health.config.ThrottleConfig
import surge.health.domain.{ EmittableHealthSignal, Error, HealthSignal, HealthSignalSource, Timed, Trace, Warning }
import surge.health.matchers.SignalPatternMatcherDefinition
import surge.health.supervisor.Api.{ RegisterSupervisedComponentRequest, RestartComponent, ShutdownComponent, StartComponent }
import surge.health.supervisor.Domain.SupervisedComponentRegistration
import surge.internal.health.RegistrationHandler

import java.time.Instant
import java.util.UUID
import java.util.regex.Pattern
import scala.concurrent.Future
import scala.util.Try

trait InvokableHealthRegistration {
  def invoke(): Future[Ack]

  def underlyingRegistration(): HealthRegistration
}

trait HealthSignalBusAware {
  def signalBus(): HealthSignalBusTrait
}

trait HealthyPublisher extends HealthSignalBusAware {
  def signalMetadata(): Map[String, String] = Map.empty
}

trait RegistrationConsumer {
  def registrations(): Future[Seq[SupervisedComponentRegistration]]
  def registrations(matching: Pattern): Future[Seq[SupervisedComponentRegistration]]
}

final case class HealthRegistration(
    componentName: String,
    control: Controllable,
    topic: String,
    restartSignalPatterns: Seq[Pattern] = Seq.empty,
    shutdownSignalPatterns: Seq[Pattern] = Seq.empty,
    id: UUID = UUID.randomUUID(),
    timestamp: Instant = Instant.now,
    ref: Option[ActorRef] = None)
    extends HealthMessage

trait RegistrationProducer {
  def register(control: Controllable, componentName: String, restartSignalPatterns: Seq[Pattern], shutdownSignalPatterns: Seq[Pattern] = Seq.empty): Future[Ack]
  def unregister(control: Controllable, componentName: String): Future[Ack]
  def registration(
      controllable: Controllable,
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
  private val log = LoggerFactory.getLogger(getClass)
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

sealed trait HealthSupervisionEvent extends NoSerializationVerificationNeeded {}
case class HealthRegistrationReceived(registration: RegisterSupervisedComponentRequest) extends HealthSupervisionEvent
case class HealthSignalReceived(signal: HealthSignal) extends HealthSupervisionEvent
case class HealthSignalStreamAdvanced() extends HealthSupervisionEvent
case class RestartComponentAttempted(componentName: String, timestamp: Instant = Instant.now()) extends HealthSupervisionEvent
case class RestartComponentFailed(componentName: String, timestamp: Instant = Instant.now(), error: Option[Throwable]) extends HealthSupervisionEvent
case class ShutdownComponentFailed(componentName: String, timestamp: Instant = Instant.now(), error: Option[Throwable]) extends HealthSupervisionEvent
case class ComponentRestarted(componentName: String, timestamp: Instant = Instant.now()) extends HealthSupervisionEvent
case class ComponentShutdown(componentName: String, timestamp: Instant = Instant.now()) extends HealthSupervisionEvent

case class ShutdownComponentAttempted(componentName: String, timestamp: Instant = Instant.now()) extends HealthSupervisionEvent

case class HealthSupervisorState(started: Boolean)
case class ControlProxy(name: String, actor: ActorRef) {
  def shutdown(replyTo: ActorRef): Unit = {
    actor ! ShutdownComponent(name, replyTo)
  }

  def restart(replyTo: ActorRef): Unit = {
    actor ! RestartComponent(name, replyTo)
  }

  def start(replyTo: ActorRef): Unit = {
    actor ! StartComponent(name, replyTo)
  }
}

case class HealthRegistrationLink(componentName: String, controlProxy: ControlProxy)

trait HealthSupervisorTrait {
  def state(): HealthSupervisorState
  def stop(): HealthSupervisorTrait
  def start(replyTo: Option[ActorRef] = None): HealthSupervisorTrait

  def register(registration: HealthRegistration): Future[Any]
  def unregister(componentName: String): Future[Ack]
  def registrar(): ActorRef

  def registrationLinks(): Seq[HealthRegistrationLink]
  def actorSystem(): ActorSystem
}

trait SignalProducer {
  def signalWithError(name: String, error: Error, metadata: Map[String, String] = Map.empty): EmittableHealthSignal
  def signalWithWarning(name: String, warning: Warning, metadata: Map[String, String] = Map.empty): EmittableHealthSignal
  def signalWithTrace(name: String, trace: Trace, metadata: Map[String, String] = Map.empty): EmittableHealthSignal
}

trait HealthSignalBusTrait extends EventBus with BusSupervisionTrait with SignalProducer with HealthSignalSource {
  type Event = HealthMessage
  type Classifier = String
  type Subscriber = HealthListener

  def supervise(): HealthSignalBusTrait

  def unsupervise(): HealthSignalBusTrait

  def registrationTopic(): String
  def signalTopic(): String

  def supervisor(): Option[HealthSupervisorTrait]

  def signalStream(): HealthSignalStream

  def asSink(): Sink[HealthMessage, Future[Done]] = Sink.foreach(message => {
    publish(message)
  })

  def asSource(buffer: Int, throttleConfig: ThrottleConfig): SourcePlusQueue[HealthMessage] =
    signalStream().messageSource(buffer, throttleConfig)

  def asSignalSource(buffer: Int, throttleConfig: ThrottleConfig): SourcePlusQueue[HealthSignal] =
    signalStream().signalSource(buffer, throttleConfig)
}

trait HealthListener extends Comparable[String] {
  def id(): String

  def handleMessage(message: HealthMessage): Unit

  override def toString: String = id()

  override def compareTo(o: String): Int = {
    o.compareTo(this.id())
  }
}

trait HealthSignalListener extends HealthListener {
  private val log = LoggerFactory.getLogger(getClass)
  private var handler: SignalHandler = _
  def signalBus(): HealthSignalBusTrait

  def signalTopic: String = signalBus().signalTopic()
  def bindSignalHandler(handler: SignalHandler): Unit = {
    this.handler = handler
  }

  def handleSignal(signal: HealthSignal): Unit = {
    Option(this.handler).foreach(h => h.handle(signal))
  }

  override def handleMessage(message: HealthMessage): Unit = {
    message match {
      case signal: HealthSignal =>
        handleSignal(signal)
      case other =>
        log.error(s"Unable to handle message of type ${other.getClass}")
    }
  }

  def subscribe(signalHandler: SignalHandler): HealthSignalListener

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

trait SignalSourceQueueProvider {
  def signalSourceQueue(): Option[SourceQueueWithComplete[HealthSignal]]
}

class SourceQueueBackedSignalHandler(actorSystem: ActorSystem) extends SignalHandler with SignalSourceQueueProvider {
  private var backingQueue: Option[SourceQueueWithComplete[HealthSignal]] = None
  def queue: Option[SourceQueueWithComplete[HealthSignal]] = backingQueue

  def bindQueue(queue: SourceQueueWithComplete[HealthSignal]): Unit =
    backingQueue = Some(queue)

  def unbindQueue(): Unit = {
    backingQueue = None
  }

  override def signalSourceQueue(): Option[SourceQueueWithComplete[HealthSignal]] = backingQueue

  override def handle(signal: HealthSignal): Try[Done] = {
    Try {
      queue.foreach(q => q.offer(signal))
      Done
    }
  }
}

case class SourcePlusQueue[T](source: Source[T, NotUsed], queue: SourceQueueWithComplete[T])

trait HealthSignalStreamControl {
  def stop(): Unit
}

trait HealthSignalStream extends HealthSignalListener {
  def actorSystem: ActorSystem
  def signalTopic: String

  /**
   * Get SignalHandler
   * @see
   *   surge.health.HealthSignalStream#subscribe
   * @return
   *   SignalHandler
   */
  def signalHandler: SignalHandler

  def patternMatchers(): Seq[SignalPatternMatcherDefinition]

  /**
   * Provide HealthSignal(s) in a Source for stream operations
   *
   * @param buffer
   *   Int
   * @param throttleConfig
   *   ThrottleConfig
   * @return
   *   SourcePlusQueue[HealthSignal]
   */
  def signalSource(buffer: Int, throttleConfig: ThrottleConfig): SourcePlusQueue[HealthSignal] = {
    val signalSource = Source.queue[HealthSignal](buffer, throttleConfig.overflowStrategy()).throttle(throttleConfig.elements, throttleConfig.duration)

    val (sourceMat, source) = signalSource.preMaterialize()(Materializer(actorSystem))

    SourcePlusQueue(source, sourceMat)
  }

  def messageSource(buffer: Int, throttleConfig: ThrottleConfig): SourcePlusQueue[HealthMessage] = {
    val signalSource = Source.queue[HealthMessage](buffer, throttleConfig.overflowStrategy()).throttle(throttleConfig.elements, throttleConfig.duration)

    val (sourceMat, source) = signalSource.preMaterialize()(Materializer(actorSystem))

    SourcePlusQueue(source, sourceMat)
  }

  /**
   * Subscribe for HealthSignals
   * @return
   *   HealthSignalStream
   */
  def subscribe(): HealthSignalStream = {
    subscribe(signalHandler).asInstanceOf[HealthSignalStream]
  }
}
