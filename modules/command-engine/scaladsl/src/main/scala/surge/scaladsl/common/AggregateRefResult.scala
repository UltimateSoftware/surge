// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.common

sealed trait CommandResult[Reply]
case class CommandSuccess[Reply](reply: Option[Reply]) extends CommandResult[Reply]
case class CommandFailure[Reply](reason: Throwable) extends CommandResult[Reply]

sealed trait ApplyEventResult[Reply]
case class ApplyEventSuccess[Reply](reply: Option[Reply]) extends ApplyEventResult[Reply]
case class ApplyEventFailure[Reply](reason: Throwable) extends ApplyEventResult[Reply]
