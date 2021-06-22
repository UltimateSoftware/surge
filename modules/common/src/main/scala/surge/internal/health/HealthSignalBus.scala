// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import java.util.regex.Pattern

import akka.actor.{ Actor, ActorSystem, Props }
import akka.event.LookupClassification
import akka.pattern._
import com.typesafe.config.{ Config, ConfigFactory }
import org.slf4j.{ Logger, LoggerFactory }
import surge.core.{ Controllable, ControllableWithHooks }
import surge.health._
import surge.health.config.HealthSignalBusConfig
import surge.health.domain.{ EmittableHealthSignal, Error, HealthRegistration, HealthSignal, Trace, Warning }
import surge.internal.health.HealthSignalBus.log
import surge.internal.health.supervisor._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.languageFeature.postfixOps
import scala.util.Try

case class SubscriberInfo(name: String, id: String)

object HealthSignalBus {
  implicit val system: ActorSystem = ActorSystem("HealthSignalBusActorSystem")
  val log: Logger = LoggerFactory.getLogger(getClass)

  val config: Config = ConfigFactory.load().atPath("surge.health")

  def apply(signalStream: HealthSignalStreamProvider, startStreamOnInit: Boolean = false): HealthSignalBusInternal = {
    val stopStreamOnUnsubscribe = startStreamOnInit
    val bus = new HealthSignalBusImpl(
      HealthSignalBusConfig(
        signalTopic = Try { config.getString("bus.signalTopic") }.getOrElse("health.signal"),
        registrationTopic = Try { config.getString("bus.registrationTopic") }.getOrElse("health.registration"),
        allowedSubscriberCount = Try { config.getInt("bus.allowedSubscriberCount") }.getOrElse(128)),
      signalStream,
      stopStreamOnUnsubscribe)

    if (startStreamOnInit) {
      bus.signalStream().start()
    }

    bus
  }
}

trait HealthSignalBusInternal extends HealthSignalBusTrait with LookupClassification {
  def subscriberInfo(): Set[SubscriberInfo]
  def withStreamSupervision(
      provider: HealthSignalBusInternal => HealthSupervisorActorRef,
      monitorRef: Option[StreamMonitoringRef] = None): HealthSignalBusInternal
}

private class InvokableHealthRegistrationImpl(healthRegistration: HealthRegistration, supervisor: HealthSupervisorTrait, signalBus: HealthSignalBusTrait)
    extends InvokableHealthRegistration {
  override def invoke(): Future[Ack] = {
    supervisor.register(healthRegistration).map(a => a.asInstanceOf[Ack])(supervisor.actorSystem().dispatcher)
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
        logger.debug(healthSignal.data.description)
      case "error" =>
        logger.error(healthSignal.data.description)
      case "warn" =>
        logger.warn(healthSignal.data.description)
      case "trace" =>
        logger.trace(healthSignal.data.description)
      case _ =>
        logger.debug(healthSignal.data.description)
    }
    this
  }
}

private[surge] class HealthSignalBusImpl(config: HealthSignalBusConfig, signalStreamSupplier: HealthSignalStreamProvider, stopStreamOnUnsubscribe: Boolean)
    extends HealthSignalBusInternal {
  implicit val postfix: postfixOps = postfixOps
  implicit val actorSystem: ActorSystem = ActorSystem("healthSignalBusActorSystem")

  private lazy val stream: HealthSignalStream = signalStreamSupplier.provide(bus = this).subscribe()
  private var supervisorRef: Option[HealthSupervisorActorRef] = None
  private var monitoringRef: Option[HealthSignalStreamMonitoringRefWithSupervisionSupport] = None

  override protected def mapSize(): Int = 128
  override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int =
    a.compareTo(b.id())

  override protected def classify(event: Event): Classifier = event.topic()

  override protected def publish(event: Event, subscriber: Subscriber): Unit = {
    subscriber.handleMessage(event)
  }

  //Note: Bug Fix for EventBus - for NullPointerException when no subscribers exist that match the classification.
  override def publish(event: Event): Unit = {
    val i = Try { subscribers.valueIterator(classify(event)) }.toOption.getOrElse(Seq.empty.iterator)
    while (i.hasNext) publish(event, i.next())
  }

  override def signalTopic(): String = config.signalTopic

  override def registrationTopic(): String = config.registrationTopic

  override def withStreamSupervision(
      provider: HealthSignalBusInternal => HealthSupervisorActorRef,
      monitorRef: Option[StreamMonitoringRef]): HealthSignalBusInternal = {
    val monitoring: HealthSignalStreamMonitoringRefWithSupervisionSupport = monitorRef match {
      case Some(m) => HealthSignalStreamMonitoringRefWithSupervisionSupport.from(m)
      case None =>
        val actor = actorSystem.actorOf(Props(new Actor() {
          override def receive: Receive = {
            case HealthRegistrationReceived(registration: HealthRegistration) =>
              log.debug("Health Registration received {}", registration)
            case HealthSignalReceived(signal: HealthSignal) =>
              log.debug("Health Signal received {}", signal)
            case Stop  => context.stop(self)
            case other => log.debug(s"Unhandled message $other")
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
    supervisor()
      .map(s => {
        if (!s.state().started) {
          s.start(monitoringRef.map(ref => ref.actor))
        }
        this
      })
      .getOrElse(this)
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
      //ref: ActorRef,
      control: Controllable,
      componentName: String,
      restartSignalPatterns: Seq[Pattern],
      shutdownSignalPatterns: Seq[Pattern] = Seq.empty): Future[Ack] = {
    registration(control, componentName, restartSignalPatterns, shutdownSignalPatterns).invoke()
  }

  override def registration(
      //ref: ActorRef,
      control: Controllable,
      componentName: String,
      restartSignalPatterns: Seq[Pattern],
      shutdownSignalPatterns: Seq[Pattern]): InvokableHealthRegistration = {
    supervisor() match {
      case Some(exists) =>
        new InvokableHealthRegistrationImpl(
          HealthRegistration(control, config.registrationTopic, componentName, restartSignalPatterns, shutdownSignalPatterns),
          exists,
          signalBus = this)
      case None =>
        throw new RuntimeException("missing Health Supervisor")
    }

  }

  override def registrations(): Future[Seq[HealthRegistration]] = {
    supervisorRef match {
      case Some(ref) =>
        val result = ref.actor.ask(HealthRegistrationRequest)(timeout = 10.seconds)

        result.map(a => a.asInstanceOf[List[HealthRegistration]])(actorSystem.dispatcher)
      case None => Future.successful(Seq.empty)
    }
  }

  override def signalWithError(name: String, error: Error, metadata: Map[String, String] = Map.empty): EmittableHealthSignal = {
    val signal = HealthSignal(topic = config.signalTopic, name = name, data = error, metadata = metadata, signalType = SignalType.ERROR)
    new EmittableHealthSignalImpl(signal, signalBus = this)
  }

  override def signalWithWarning(name: String, warning: Warning, metadata: Map[String, String] = Map.empty): EmittableHealthSignal = {
    val signal = HealthSignal(topic = config.signalTopic, name = name, data = warning, metadata = metadata, signalType = SignalType.WARNING)
    new EmittableHealthSignalImpl(signal, signalBus = this)
  }

  override def signalWithTrace(name: String, trace: Trace, metadata: Map[String, String] = Map.empty): EmittableHealthSignal = {
    val signal = HealthSignal(topic = config.signalTopic, name = name, data = trace, metadata = metadata, signalType = SignalType.TRACE)
    new EmittableHealthSignalImpl(signal, signalBus = this)
  }

  override def subscriberInfo(): Set[SubscriberInfo] =
    subscribers.values.map(s => SubscriberInfo(s.getClass.getName, s.hashCode().toString))

  override def signalStream(): HealthSignalStream = stream
}
