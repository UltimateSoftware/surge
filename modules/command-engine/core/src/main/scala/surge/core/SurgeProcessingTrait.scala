// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.internal.SurgeModel

trait SurgeProcessingTrait[State, Message, +Rejection, Event, Response] extends Controllable {
  val businessLogic: SurgeModel[State, Message, Rejection, Event, Response]
  def actorSystem: ActorSystem
  def config: Config
}
