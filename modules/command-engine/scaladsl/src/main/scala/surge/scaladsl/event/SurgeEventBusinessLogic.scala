// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import com.typesafe.config.Config
import surge.core.commondsl.AbstractSurgeEventBusinessLogic

abstract class SurgeEventBusinessLogic[AggId, Agg, Event](config: Config) extends AbstractSurgeEventBusinessLogic[AggId, Agg, Event](config)
