// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

/**
 * Calculates a per-second average across the given time period
 * @param expirationTimeSeconds The number of seconds to keep sensor readings for and average across
 */
class RateHistogram(expirationTimeSeconds: Long) extends TimeBucketHistogram with PerSecondHistogramBucketStrategy {
  override def numberOfBuckets: Long = expirationTimeSeconds
}
