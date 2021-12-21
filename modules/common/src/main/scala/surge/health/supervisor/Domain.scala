// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.supervisor

import akka.actor.ActorRef

import java.util.UUID
import java.util.regex.Pattern

object Domain {
  case class SupervisedComponentRegistration(
      id: UUID,
      componentName: String,
      controlProxyRef: ActorRef,
      restartSignalPatterns: Seq[Pattern],
      shutdownSignalPatterns: Seq[Pattern])
}
