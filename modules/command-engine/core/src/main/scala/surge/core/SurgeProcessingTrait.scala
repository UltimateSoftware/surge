// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.ActorSystem
import com.typesafe.config.Config

trait SurgeProcessingTrait[S, M, +R, E] {
  def start(): Unit
  def restart(): Unit
  def stop(): Unit
  val businessLogic: SurgeModel[S, E]
  def actorSystem: ActorSystem
  def config: Config
}
