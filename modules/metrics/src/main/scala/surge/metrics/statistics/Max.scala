// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

import surge.metrics.MetricValueProvider

/**
 * Record the maximum observed value over the full lifetime of this object.
 */
class Max extends MetricValueProvider {
  private var currentMax: Option[Double] = None
  override def update(value: Double, timestampMs: Long): Unit = {
    currentMax = Some(Math.max(value, currentMax.getOrElse(value)))
  }
  override def getValue: Double = currentMax.getOrElse(0.0)
}
