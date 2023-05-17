// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.scaladsl
package event

object Types {
  type ApplyEventResult[Agg] = common.ApplyEventResult[Agg]
  type ApplyEventsSuccess[Agg] = common.ApplyEventSuccess[Agg]
  val ApplyEventSuccess: common.ApplyEventSuccess.type = common.ApplyEventSuccess
  type ApplyEventFailure[Agg] = common.ApplyEventFailure[Agg]
  val ApplyEventFailure: common.ApplyEventFailure.type = common.ApplyEventFailure
}
