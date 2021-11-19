// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.internal.health

import akka.actor.{ ActorRef, NoSerializationVerificationNeeded }
import surge.core.Controllable
import surge.health.domain.{ HealthRegistration, HealthSignal, HealthSignalSource, SignalData, Timed }
import surge.health.supervisor.Api.{ RegisterSupervisedComponentRequest, RestartComponent, ShutdownComponent, StartComponent }
import surge.health.{ ControlProxy, HealthRegistrationLink, HealthSupervisorState, SignalType }

import java.time.Instant
import java.util.UUID
import java.util.regex.Pattern

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

case class HealthSupervisorStateImpl(started: Boolean) extends HealthSupervisorState

final case class HealthRegistrationImpl(
    componentName: String,
    control: Controllable,
    topic: String,
    override val restartSignalPatterns: Seq[Pattern] = Seq.empty,
    override val shutdownSignalPatterns: Seq[Pattern] = Seq.empty,
    id: UUID = UUID.randomUUID(),
    timestamp: Instant = Instant.now,
    override val ref: Option[ActorRef] = None)
    extends HealthRegistration
    with NoSerializationVerificationNeeded

final case class HealthSignalImpl(
    topic: String,
    name: String,
    signalType: SignalType.Value,
    data: SignalData,
    override val metadata: Map[String, String] = Map[String, String](),
    source: Option[HealthSignalSource],
    override val handled: Boolean = false,
    id: UUID = UUID.randomUUID(),
    timestamp: Instant = Instant.now())
    extends HealthSignal
    with NoSerializationVerificationNeeded
    with Timed {

  override def handled(h: Boolean): HealthSignal = copy(handled = h)
  override def data(d: SignalData): HealthSignal = copy(data = d)
  override def source(s: Option[HealthSignalSource]): HealthSignal = copy(source = s)
}

case class ControlProxyImpl(name: String, actor: ActorRef) extends ControlProxy {
  override def shutdown(replyTo: ActorRef): Unit = {
    actor ! ShutdownComponent(name, replyTo)
  }

  override def restart(replyTo: ActorRef): Unit = {
    actor ! RestartComponent(name, replyTo)
  }

  override def start(replyTo: ActorRef): Unit = {
    actor ! StartComponent(name, replyTo)
  }
}

case class HealthRegistrationLinkImpl(componentName: String, controlProxy: ControlProxy) extends HealthRegistrationLink
