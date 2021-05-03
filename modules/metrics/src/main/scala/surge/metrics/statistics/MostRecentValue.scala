// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

import surge.metrics.MetricValueProvider

/**
 * Keeps track of the most recently observed value by using the observed timestamp to determine ordering.
 */
class MostRecentValue extends MetricValueProvider {
  private var mostRecentTimeValue: (Long, Double) = 0L -> 0.0
  override def update(value: Double, timestampMs: Long): Unit = {
    this.synchronized {
      if (timestampMs >= mostRecentTimeValue._1) {
        mostRecentTimeValue = timestampMs -> value
      }
    }

  }

  override def getValue: Double = mostRecentTimeValue._2
}
