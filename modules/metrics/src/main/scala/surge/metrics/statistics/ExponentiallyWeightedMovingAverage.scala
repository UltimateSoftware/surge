// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

import surge.metrics.MetricValueProvider

/**
 * Calculate an exponentially weighted moving average of observed values. The given weight is the weighting given
 * to current value when calculating a new average from an observed value.
 * @param weight The weight (between 0.0 and 1.0) given to the current average when applying a new observed value
 */
class ExponentiallyWeightedMovingAverage(weight: Double) extends MetricValueProvider {
  private var currentEWMA = 0.0

  override def update(value: Double, timestampMs: Long): Unit = {
    this.synchronized {
      if (currentEWMA == 0.0) {
        currentEWMA = value
      } else {
        currentEWMA = (currentEWMA * weight) + (value * (1 - weight))
      }
    }
  }

  override def getValue: Double = {
    currentEWMA
  }
}
