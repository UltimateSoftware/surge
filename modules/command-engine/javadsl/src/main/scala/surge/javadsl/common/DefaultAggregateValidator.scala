// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.common

import java.util.Optional

trait DefaultAggregateValidator {
  @deprecated("Aggregate validation in the KTable is no longer supported", "0.5.12")
  def aggregateValidator(key: String, aggJson: Array[Byte], prevAggJsonOpt: Optional[Array[Byte]]): Boolean = true
}
