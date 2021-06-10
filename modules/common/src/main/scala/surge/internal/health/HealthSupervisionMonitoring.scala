// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import akka.actor.ActorRef

trait HealthSupervisionMonitoring {
  def actor(): ActorRef
}
