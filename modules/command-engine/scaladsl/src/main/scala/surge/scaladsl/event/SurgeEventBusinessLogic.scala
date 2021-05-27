// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import surge.core.commondsl.SurgeEventBusinessLogicTrait
import surge.scaladsl.common.DefaultAggregateValidator

abstract class SurgeEventBusinessLogic[AggId, Agg, Event] extends SurgeEventBusinessLogicTrait[AggId, Agg, Event] with DefaultAggregateValidator
