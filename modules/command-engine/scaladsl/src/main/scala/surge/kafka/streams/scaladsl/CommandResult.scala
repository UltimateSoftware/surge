// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams.scaladsl

sealed trait CommandResult[Agg]
case class CommandSuccess[Agg](aggregateState: Option[Agg]) extends CommandResult[Agg]
case class CommandFailure[Agg](reason: Throwable) extends CommandResult[Agg]

sealed trait ApplyEventResult[Agg]
case class ApplyEventsSuccess[Agg](aggregateState: Option[Agg]) extends ApplyEventResult[Agg]
case class ApplyEventsFailure[Agg](reason: Throwable) extends ApplyEventResult[Agg]
