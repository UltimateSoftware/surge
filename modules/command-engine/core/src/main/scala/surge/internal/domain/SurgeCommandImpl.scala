// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core.command.SurgeCommandModel
import surge.internal.health.HealthSignalStreamProvider

private[surge] abstract class SurgeCommandImpl[Agg, Command, +Rej, Event, Response](
    actorSystem: ActorSystem,
    businessLogic: SurgeCommandModel[Agg, Command, Rej, Event, Response],
    signalStreamProvider: HealthSignalStreamProvider,
    config: Config)
    extends SurgeMessagePipeline[Agg, Command, Rej, Event, Response](actorSystem, businessLogic, signalStreamProvider, config)
