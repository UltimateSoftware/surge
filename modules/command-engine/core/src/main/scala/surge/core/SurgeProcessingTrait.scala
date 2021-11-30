// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.internal.SurgeModel

trait SurgeProcessingTrait[S, M, +R, E] {
  def controllable: Controllable
  val businessLogic: SurgeModel[S, M, R, E]
  def actorSystem: ActorSystem
  def config: Config
}
