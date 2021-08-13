// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import surge.core.commondsl.SurgeCommandBusinessLogicTrait
import surge.javadsl.common.DefaultAggregateValidator

abstract class SurgeCommandBusinessLogic[AggId, Agg, Command, Event, Response]
    extends SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Event, Response]
    with DefaultAggregateValidator {

  @deprecated("Will be removed from here once an alternative model to CQRS processing is available.", "0.5.2")
  override def publishStateOnly: Boolean = false

}
