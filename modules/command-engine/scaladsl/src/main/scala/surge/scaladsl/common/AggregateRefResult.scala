// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.common

sealed trait CommandResult[Agg]
case class CommandSuccess[Agg](aggregateState: Option[Agg]) extends CommandResult[Agg]
case class CommandFailure[Agg](reason: Throwable) extends CommandResult[Agg]

sealed trait ApplyEventResult[Agg]
case class ApplyEventSuccess[Agg](aggregateState: Option[Agg]) extends ApplyEventResult[Agg]
case class ApplyEventFailure[Agg](reason: Throwable) extends ApplyEventResult[Agg]
