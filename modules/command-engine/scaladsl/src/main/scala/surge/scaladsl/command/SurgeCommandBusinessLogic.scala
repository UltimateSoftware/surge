// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import surge.core.commondsl.SurgeCommandBusinessLogicTrait
import surge.scaladsl.common.DefaultAggregateValidator

abstract class SurgeCommandBusinessLogic[AggId, Agg, Command, Event]
    extends SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event, Agg]
    with DefaultAggregateValidator {

  @deprecated("Will be removed from here once an alternative model to CQRS processing is available.", "0.5.2")
  override def publishStateOnly: Boolean = false
}
