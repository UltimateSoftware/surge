// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.common

import java.util.Optional
import scala.compat.java8.OptionConverters._

trait DefaultAggregateValidator {
  def aggregateValidator(key: String, aggJson: Array[Byte], prevAggJsonOpt: Optional[Array[Byte]]): Boolean = true

  protected[surge] final def aggregateValidatorLambda: (String, Array[Byte], Option[Array[Byte]]) => Boolean =
    (key: String, aggJson: Array[Byte], prevAggJsonOpt: Option[Array[Byte]]) => {
      aggregateValidator(key, aggJson, prevAggJsonOpt.asJava)
    }

}
