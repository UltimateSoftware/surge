// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.common

trait DefaultAggregateValidator {
  def aggregateValidator(key: String, aggJson: Array[Byte], prevAggJsonOpt: Option[Array[Byte]]): Boolean = true

  protected[surge] final def aggregateValidatorLambda: (String, Array[Byte], Option[Array[Byte]]) => Boolean = aggregateValidator
}
