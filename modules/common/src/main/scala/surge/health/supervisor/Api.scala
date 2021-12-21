// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.supervisor

import akka.actor.{ ActorRef, NoSerializationVerificationNeeded }
import surge.health.supervisor.Domain.SupervisedComponentRegistration

import java.util.UUID
import java.util.regex.Pattern

object Api {
  // Lifecycle Control
  case class Start(replyTo: Option[ActorRef] = None) extends NoSerializationVerificationNeeded
  case class Stop() extends NoSerializationVerificationNeeded

  case class StartComponent(name: String, replyTo: ActorRef) extends NoSerializationVerificationNeeded
  case class RestartComponent(name: String, replyTo: ActorRef) extends NoSerializationVerificationNeeded
  case class QueryComponentExists(name: String) extends NoSerializationVerificationNeeded
  case class ShutdownComponent(name: String, replyTo: ActorRef) extends NoSerializationVerificationNeeded

  case class UnregisterSupervisedComponentRequest(componentName: String) extends NoSerializationVerificationNeeded
  case class RegisterSupervisedComponentRequest(
      id: UUID,
      componentName: String,
      controlProxyRef: ActorRef,
      restartSignalPatterns: Seq[Pattern],
      shutdownSignalPatterns: Seq[Pattern])
      extends NoSerializationVerificationNeeded {
    def asSupervisedComponentRegistration(): SupervisedComponentRegistration =
      SupervisedComponentRegistration(id, componentName, controlProxyRef, restartSignalPatterns, shutdownSignalPatterns)
  }

  case class HealthRegistrationDetailsRequest() extends NoSerializationVerificationNeeded
}
