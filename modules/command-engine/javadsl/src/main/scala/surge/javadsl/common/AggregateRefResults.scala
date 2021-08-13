// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.common

import java.util.Optional

sealed trait CommandResult[Reply]
case class CommandSuccess[Reply](reply: Optional[Reply]) extends CommandResult[Reply]
case class CommandFailure[Reply](reason: Throwable) extends CommandResult[Reply]

sealed trait ApplyEventResult[Reply]
case class ApplyEventSuccess[Reply](reply: Optional[Reply]) extends ApplyEventResult[Reply]
case class ApplyEventFailure[Reply](reason: Throwable) extends ApplyEventResult[Reply]
