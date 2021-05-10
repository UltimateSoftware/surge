// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import com.typesafe.config.Config
import surge.core.commondsl.SurgeCommandBusinessLogicTrait
import surge.scaladsl.common.DefaultAggregateValidator

abstract class SurgeCommandBusinessLogic[AggId, Agg, Command, Event](conf: Config)
    extends SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event]
    with DefaultAggregateValidator {
  override protected[surge] val config = conf
  override final def publishStateOnly: Boolean = false
}
