// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core.event.SurgeEventServiceModel

private[surge] abstract class SurgeEventServiceImpl[Agg, Event](actorSystem: ActorSystem, businessLogic: SurgeEventServiceModel[Agg, Event], config: Config)
    extends SurgeMessagePipeline[Agg, Nothing, Nothing, Event](actorSystem, businessLogic, config)
