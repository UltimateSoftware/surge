// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

import surge.metrics.MetricValueProvider

/**
 * Record the minimum observed value over the full lifetime of this object.
 */
class Min extends MetricValueProvider {
  private var currentMin: Option[Double] = None
  override def update(value: Double, timestampMs: Long): Unit = {
    currentMin = Some(Math.min(value, currentMin.getOrElse(value)))
  }
  override def getValue: Double = currentMin.getOrElse(0.0)
}
