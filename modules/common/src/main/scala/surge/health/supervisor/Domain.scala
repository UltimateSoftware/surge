// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.supervisor

import akka.actor.ActorRef

import java.util.regex.Pattern

object Domain {
  case class SupervisedComponentRegistration(
      componentName: String,
      controlProxyRef: ActorRef,
      restartSignalPatterns: Seq[Pattern],
      shutdownSignalPatterns: Seq[Pattern])
}
