// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core.event.SurgeEventServiceModel
import surge.internal.health.HealthSignalStreamProvider

private[surge] abstract class SurgeEventServiceImpl[Agg, Event, Response](
    actorSystem: ActorSystem,
    businessLogic: SurgeEventServiceModel[Agg, Event, Response],
    signalStreamProvider: HealthSignalStreamProvider,
    config: Config)
    extends SurgeMessagePipeline[Agg, Nothing, Nothing, Event, Response](actorSystem, businessLogic, signalStreamProvider, config)
