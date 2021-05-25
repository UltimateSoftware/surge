// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl
package command

object Types {
  type CommandResult[Agg] = common.CommandResult[Agg]
  type CommandSuccess[Agg] = common.CommandSuccess[Agg]
  val CommandSuccess: common.CommandSuccess.type = common.CommandSuccess
  type CommandFailure[Agg] = common.CommandFailure[Agg]
  val CommandFailure: common.CommandFailure.type = common.CommandFailure
}
