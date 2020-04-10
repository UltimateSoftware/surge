// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional

sealed trait CommandResult[Agg]
case class CommandSuccess[Agg](aggregateState: Optional[Agg]) extends CommandResult[Agg]
case class CommandFailure[Agg](reason: Throwable) extends CommandResult[Agg]

sealed trait ApplyEventResult[Agg]
case class ApplyEventsSuccess[Agg](aggregateState: Optional[Agg]) extends ApplyEventResult[Agg]
case class ApplyEventsFailure[Agg](reason: Throwable) extends ApplyEventResult[Agg]
