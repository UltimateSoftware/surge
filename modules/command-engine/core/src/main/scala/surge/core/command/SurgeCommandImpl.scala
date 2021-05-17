// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.command

import akka.actor.ActorSystem
import com.typesafe.config.Config
import surge.internal.domain.SurgeMessagePipeline
import surge.kafka.KafkaTopic

abstract class SurgeCommandImpl[Agg, Command, +Rej, Event](actorSystem: ActorSystem, businessLogic: SurgeCommandModel[Agg, Command, Rej, Event], config: Config)
    extends SurgeMessagePipeline[Agg, Command, Rej, Event](actorSystem, businessLogic, config)
