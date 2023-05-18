// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.jmx

import akka.actor.ActorRef

trait HealthJmxTrait {
  def asActorRef(): ActorRef
  def getJmxActor: Option[ActorRef]
}
