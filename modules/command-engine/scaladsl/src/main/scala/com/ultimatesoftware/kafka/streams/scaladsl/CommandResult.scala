// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

sealed trait CommandResult[Agg]
case class CommandSuccess[Agg](aggregateState: Option[Agg]) extends CommandResult[Agg]
case class CommandFailure[Agg](reason: Throwable) extends CommandResult[Agg]

sealed trait ApplyEventResult[Agg]
case class ApplyEventsSuccess[Agg](aggregateState: Option[Agg]) extends ApplyEventResult[Agg]
case class ApplyEventsFailure[Agg](reason: Throwable) extends ApplyEventResult[Agg]
