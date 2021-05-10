// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import com.typesafe.config.Config
import surge.core.commondsl.SurgeCommandBusinessLogicTrait
import surge.javadsl.common.DefaultAggregateValidator

abstract class AbstractSurgeCommandBusinessLogic[AggId, Agg, Command, Rej, Event](conf: Config)
    extends SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event]
    with DefaultAggregateValidator {

  override protected[surge] val config = conf

  @deprecated("Will be removed from here once an alternative model to CQRS processing is available.", "0.5.2")
  override def publishStateOnly: Boolean = false

}
