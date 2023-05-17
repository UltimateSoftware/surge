// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.core.command.SurgeCommandModel
import surge.internal.health.HealthSignalStreamProvider

private[surge] abstract class SurgeCommandImpl[Agg, Command, Event](
    actorSystem: ActorSystem,
    businessLogic: SurgeCommandModel[Agg, Command, Event],
    signalStreamProvider: HealthSignalStreamProvider,
    config: Config)
    extends SurgeMessagePipeline[Agg, Command, Event](actorSystem, businessLogic, signalStreamProvider, config)
