// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import akka.actor.{ Actor, ActorSystem, BootstrapSetup, Props, ProviderSelection }
import akka.event.LookupClassification
import akka.pattern._
import com.typesafe.config.{ Config, ConfigFactory }
import org.slf4j.{ Logger, LoggerFactory }
import surge.core.{ Ack, Controllable }
import surge.health._
import surge.health.config.{ HealthSignalBusConfig, HealthSupervisorConfig }
import surge.health.domain.{ EmittableHealthSignal, Error, HealthSignal, HealthSignalSource, SnapshotHealthSignalSource, Trace, Warning }
import surge.health.supervisor.Api.{ HealthRegistrationDetailsRequest, RegisterSupervisedComponentRequest, Stop }
import surge.health.supervisor.Domain.SupervisedComponentRegistration
import surge.internal.health.HealthSignalBus.log
import surge.internal.health.supervisor._

import java.util.regex.Pattern
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

case class SubscriberInfo(name: String, id: String)

object HealthSignalBus {
  implicit val system: ActorSystem = ActorSystem.create("HealthSignalBusActorSystem", BootstrapSetup().withActorRefProvider(ProviderSelection.local()))
  val log: Logger = LoggerFactory.getLogger(getClass)

  val config: Config = ConfigFactory.load().getConfig("surge.health")
  val startStreamOnInit: Boolean = config.getBoolean("bus.stream.start-on-init")

  val healthSignalBusConfig: HealthSignalBusConfig = HealthSignalBusConfig(
    streamingEnabled = config.getBoolean("bus.stream.enabled"),
    signalTopic = config.getString("bus.signal-topic"),
    registrationTopic = config.getString("bus.registration-topic"),
    allowedSubscriberCount = config.getInt("bus.allowed-subscriber-count"))

  def apply(
      healthSignalBusConfig: HealthSignalBusConfig,
      signalStream: HealthSignalStreamProvider,
      startOnInit: Boolean,
      healthSupervisorConfig: HealthSupervisorConfig)(implicit ec: ExecutionContext): HealthSignalBusInternal = {
    val bus = new HealthSignalBusImpl(healthSignalBusConfig, signalStream, startOnInit, healthSupervisorConfig)

    if (startOnInit && healthSignalBusConfig.streamingEnabled) {
      bus.signalStream().start()
    }

    bus
  }

  def apply(healthSignalBusConfig: HealthSignalBusConfig, signalStream: HealthSignalStreamProvider, startOnInit: Boolean)(
      implicit ec: ExecutionContext): HealthSignalBusInternal = {
    val bus = new HealthSignalBusImpl(healthSignalBusConfig, signalStream, startOnInit, HealthSupervisorConfig())

    if (startOnInit && healthSignalBusConfig.streamingEnabled) {
      bus.signalStream().start()
    }

    bus
  }

  def apply(signalStream: HealthSignalStreamProvider, healthSupervisorConfig: HealthSupervisorConfig = HealthSupervisorConfig())(
      implicit ec: ExecutionContext): HealthSignalBusInternal = {
    HealthSignalBus(healthSignalBusConfig, signalStream, startStreamOnInit, healthSupervisorConfig)
  }

  def apply(signalStream: HealthSignalStreamProvider)(implicit ec: ExecutionContext): HealthSignalBusInternal = {
    HealthSignalBus(healthSignalBusConfig, signalStream, startStreamOnInit, HealthSupervisorConfig())
  }
}

trait HealthSignalBusInternal extends HealthSignalBusTrait with LookupClassification {
  def subscriberInfo(): Set[SubscriberInfo]
  def backingSignalStream(): Option[HealthSignalStream]
  def withStreamSupervision(
      provider: HealthSignalBusInternal => HealthSupervisorActorRef,
      monitorRef: Option[StreamMonitoringRef] = None): HealthSignalBusInternal
}

private class InvokableHealthRegistrationImpl(healthRegistration: HealthRegistration, supervisor: HealthSupervisorTrait, signalBus: HealthSignalBusTrait)(
    implicit ec: ExecutionContext)
    extends InvokableHealthRegistration {

  override def invoke(): Future[Ack] = {
    supervisor.register(healthRegistration).map(a => a.asInstanceOf[Ack])
  }

  override def underlyingRegistration(): HealthRegistration = healthRegistration
}

object NoopInvokableHealthRegistration {
  val log: Logger = LoggerFactory.getLogger(getClass)
}

private class NoopInvokableHealthRegistration(healthRegistration: HealthRegistration) extends InvokableHealthRegistration {
  override def invoke(): Future[Ack] = {
    Future.successful[Ack](Ack)
  }

  override def underlyingRegistration(): HealthRegistration = healthRegistration
}

private class EmittableHealthSignalImpl(healthSignal: HealthSignal, signalBus: HealthSignalBusInternal) extends EmittableHealthSignal {
  import surge.health.domain.EmittableHealthSignal._

  override def handled(handled: Boolean): EmittableHealthSignal = {
    new EmittableHealthSignalImpl(healthSignal.copy(handled = handled), signalBus)
  }

  override def emit(): EmittableHealthSignal = {
    signalBus.publish(healthSignal)
    this
  }

  override def underlyingSignal(): HealthSignal = healthSignal

  override def logAsWarning(error: Option[Throwable]): EmittableHealthSignal = {
    error match {
      case Some(err) =>
        log(logType = "warn", healthSignal.copy(data = healthSignal.data.withError(err)))
      case None =>
        log(logType = "warn", healthSignal)
    }
  }

  override def logAsDebug(): EmittableHealthSignal = {
    log(logType = "debug", healthSignal)
  }

  override def logAsError(error: Option[Throwable]): EmittableHealthSignal = {
    error match {
      case Some(err) =>
        log(logType = "error", healthSignal.copy(data = healthSignal.data.withError(err)))
      case None =>
        log(logType = "error", healthSignal)
    }
  }

  override def logAsTrace(): EmittableHealthSignal = {
    log(logType = "trace", healthSignal)
  }

  private def log(logType: String, signal: HealthSignal): EmittableHealthSignal = {
    logType match {
      case "debug" =>
        logger.debug(signal.data.description)
      case "error" =>
        logger.error(signal.data.description)
      case "warn" =>
        logger.warn(signal.data.description)
      case "trace" =>
        logger.trace(signal.data.description)
      case _ =>
        logger.debug(signal.data.description)
    }
    this
  }
}

private[surge] class HealthSignalBusImpl(
    config: HealthSignalBusConfig,
    signalStreamSupplier: HealthSignalStreamProvider,
    stopStreamOnUnsubscribe: Boolean,
    healthSupervisorConfig: HealthSupervisorConfig = HealthSupervisorConfig())(implicit ec: ExecutionContext)
    extends HealthSignalBusInternal {
  implicit val actorSystem: ActorSystem =
    ActorSystem.create(name = "healthSignalBusActorSystem", BootstrapSetup().withActorRefProvider(ProviderSelection.local()))

  private lazy val stream: HealthSignalStream = if (config.streamingEnabled) {
    signalStreamSupplier.provide(bus = this).subscribe()
  } else {
    new DisabledHealthSignalStreamProvider(bus = this, actorSystem).provide(bus = this).subscribe()
  }

  private val buffer: CircularBuffer[Event] = new CircularBuffer[Event](25)

  private var supervisorRef: Option[HealthSupervisorActorRef] = None
  private var monitoringRef: Option[HealthSignalStreamMonitoringRefWithSupervisionSupport] = None

  override protected def mapSize(): Int = 128
  override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int =
    a.compareTo(b.id())

  override protected def classify(event: Event): Classifier = event.topic()

  override protected def publish(event: Event, subscriber: Subscriber): Unit = {
    buffer.push(event)
    subscriber.handleMessage(event)
  }

  override def signals(): Seq[HealthSignal] =
    buffer.getAll.toSeq.filter(e => e.isInstanceOf[HealthSignal]).map(s => s.asInstanceOf[HealthSignal])

  override def flush(): Unit = {
    buffer.clear()
  }

  // Note: Bug Fix for EventBus - for NullPointerException when no subscribers exist that match the classification.
  override def publish(event: Event): Unit = {
    val i = Try { subscribers.valueIterator(classify(event)) }.toOption.getOrElse(Seq.empty.iterator)
    while (i.hasNext) publish(event, i.next())
  }

  override def signalTopic(): String = config.signalTopic

  override def registrationTopic(): String = config.registrationTopic

  override def backingSignalStream(): Option[HealthSignalStream] = {
    Some(stream)
  }

  override def withStreamSupervision(
      provider: HealthSignalBusInternal => HealthSupervisorActorRef,
      monitorRef: Option[StreamMonitoringRef]): HealthSignalBusInternal = {
    val monitoring: HealthSignalStreamMonitoringRefWithSupervisionSupport = monitorRef match {
      case Some(m) => HealthSignalStreamMonitoringRefWithSupervisionSupport.from(m)
      case None =>
        val actor = actorSystem.actorOf(Props(new Actor {
          override def receive: Receive = {
            case HealthRegistrationReceived(registration: RegisterSupervisedComponentRequest) =>
              log.debug("Health Registration received {}", registration)
            case HealthSignalReceived(signal: HealthSignal) =>
              log.debug("Health Signal received {}", signal)
            case Stop() => context.stop(self)
            case other  => log.debug(s"Unhandled message $other")
          }
        }))
        new HealthSignalStreamMonitoringRefWithSupervisionSupport(actor)
    }

    val ref = provider(this)
    ref.start(Some(monitoring.actor))

    supervisorRef = Some(ref)
    this.monitoringRef = Some(monitoring)
    this
  }

  override def supervisor(): Option[HealthSupervisorTrait] = {
    supervisorRef
  }

  override def supervise(): HealthSignalBusTrait = {
    supervisor() match {
      case Some(s) =>
        if (!s.state().started) {
          s.start(monitoringRef.map(ref => ref.actor))
        }
        this
      case None =>
        val ref = HealthSupervisorActor(this, actorSystem, healthSupervisorConfig)
        supervisorRef = Some(ref)
        ref.start(monitoringRef.map(ref => ref.actor))
        this
    }
  }

  override def unsupervise(): HealthSignalBusTrait = {
    supervisor() match {
      case Some(s) =>
        if (s.state().started) {
          if (stopStreamOnUnsubscribe) {
            signalStream().unsubscribe().stop()
          }
          // stop the supervisor
          s.stop()

          // stop monitoring
          monitoringRef.foreach(m => m.actor ! Stop)

          supervisorRef = None
        }
      case None =>
        if (stopStreamOnUnsubscribe) {
          signalStream().unsubscribe().stop()
        }
    }
    this
  }

  override def register(
      control: Controllable,
      componentName: String,
      restartSignalPatterns: Seq[Pattern],
      shutdownSignalPatterns: Seq[Pattern] = Seq.empty): Future[Ack] = {
    registration(control, componentName, restartSignalPatterns, shutdownSignalPatterns).invoke()
  }

  override def unregister(control: Controllable, componentName: String): Future[Ack] = {
    supervisor() match {
      case Some(exists) =>
        exists.unregister(componentName)
      case None =>
        Future.successful(Ack)
    }
  }

  override def registration(
      control: Controllable,
      componentName: String,
      restartSignalPatterns: Seq[Pattern],
      shutdownSignalPatterns: Seq[Pattern]): InvokableHealthRegistration = {
    supervisor() match {
      case Some(exists) =>
        new InvokableHealthRegistrationImpl(
          HealthRegistration(
            control = control,
            topic = config.registrationTopic,
            componentName = componentName,
            restartSignalPatterns = restartSignalPatterns,
            shutdownSignalPatterns = shutdownSignalPatterns),
          exists,
          signalBus = this)
      case None =>
        log.warn(s"The Health Signal Bus is not being supervised so HealthRegistration cannot be performed for $componentName")
        new NoopInvokableHealthRegistration(
          HealthRegistration(
            control = control,
            topic = config.registrationTopic,
            componentName = componentName,
            restartSignalPatterns = restartSignalPatterns,
            shutdownSignalPatterns = shutdownSignalPatterns))
    }
  }

  override def registrations(): Future[Seq[SupervisedComponentRegistration]] = {
    supervisorRef match {
      case Some(ref) =>
        val result = ref.actor.ask(HealthRegistrationDetailsRequest())(timeout = 10.seconds)

        result.map(a => a.asInstanceOf[List[SupervisedComponentRegistration]])(actorSystem.dispatcher)
      case None => Future.successful(Seq.empty)
    }
  }

  override def registrations(matching: Pattern): Future[Seq[SupervisedComponentRegistration]] = {
    registrations().map(r => r.filter(f => matching.matcher(f.componentName).matches()))(ExecutionContext.global)
  }

  override def signalWithError(name: String, error: Error, metadata: Map[String, String] = Map.empty): EmittableHealthSignal = {
    val signal =
      HealthSignal(topic = config.signalTopic, name = name, data = error, metadata = metadata, signalType = SignalType.ERROR, source = Some(busSignalSource()))
    new EmittableHealthSignalImpl(signal, signalBus = this)
  }

  override def signalWithWarning(name: String, warning: Warning, metadata: Map[String, String] = Map.empty): EmittableHealthSignal = {
    val signal = HealthSignal(
      topic = config.signalTopic,
      name = name,
      data = warning,
      metadata = metadata,
      signalType = SignalType.WARNING,
      source = Some(busSignalSource()))
    new EmittableHealthSignalImpl(signal, signalBus = this)
  }

  override def signalWithTrace(name: String, trace: Trace, metadata: Map[String, String] = Map.empty): EmittableHealthSignal = {
    val signal =
      HealthSignal(topic = config.signalTopic, name = name, data = trace, metadata = metadata, signalType = SignalType.TRACE, source = Some(busSignalSource()))
    new EmittableHealthSignalImpl(signal, signalBus = this)
  }

  override def subscriberInfo(): Set[SubscriberInfo] =
    subscribers.values.map(s => SubscriberInfo(s.getClass.getName, s.hashCode().toString))

  override def signalStream(): HealthSignalStream = stream

  private def busSignalSource(): HealthSignalSource = new SnapshotHealthSignalSource(Seq.empty)
}
