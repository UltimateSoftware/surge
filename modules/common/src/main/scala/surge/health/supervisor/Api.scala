// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.supervisor

import akka.actor.ActorRef
import surge.health.supervisor.Domain.SupervisedComponentRegistration

import java.util.regex.Pattern

object Api {
  // Lifecycle Control
  case class Start(replyTo: Option[ActorRef] = None)
  case class Stop()

  case class StartComponent(name: String, replyTo: ActorRef)
  case class RestartComponent(name: String, replyTo: ActorRef)
  case class QueryComponentExists(name: String)
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
}
