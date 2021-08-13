// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import surge.core.commondsl.SurgeEventBusinessLogicTrait
import surge.javadsl.common.DefaultAggregateValidator

abstract class SurgeEventBusinessLogic[AggId, Agg, Event, Response]
    extends SurgeEventBusinessLogicTrait[AggId, Agg, Event, Response]
    with DefaultAggregateValidator {
  override final def publishStateOnly: Boolean = true
}
