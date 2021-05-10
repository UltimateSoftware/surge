// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.common

import java.util.Optional

sealed trait CommandResult[Agg]
case class CommandSuccess[Agg](aggregateState: Optional[Agg]) extends CommandResult[Agg]
case class CommandFailure[Agg](reason: Throwable) extends CommandResult[Agg]

sealed trait ApplyEventResult[Agg]
case class ApplyEventsSuccess[Agg](aggregateState: Optional[Agg]) extends ApplyEventResult[Agg]
case class ApplyEventsFailure[Agg](reason: Throwable) extends ApplyEventResult[Agg]
