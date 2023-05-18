// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.internal.SurgeModel

trait SurgeProcessingTrait[S, M, E] {
  private[surge] def controllable: Controllable
  val businessLogic: SurgeModel[S, M, E]
  def actorSystem: ActorSystem
  def config: Config
}
