// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

import surge.metrics.MetricValueProvider

/**
 * Records a sum of observed values over the full lifetime of this object. Can be positive or negative.
 */
class Count extends MetricValueProvider {
  private var currentCount: Double = 0.0
  override def update(value: Double, timestampMs: Long): Unit = currentCount += value
  override def getValue: Double = currentCount
}
