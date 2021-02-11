// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

import java.time.Instant

import surge.metrics.MetricValueProvider

import scala.collection.mutable

/**
 * Trait that records sensor readings into buckets calculated from the timestamp of the reading. Expires buckets with timestamps
 * that are older than the implemented bucket expiration.  The metric recorded from this is the average value across all non-expired buckets.
 */
trait TimeBucketHistogram extends MetricValueProvider {
  /**
   * Calculate a bucket for a given timestamp
   * @param timestampMs The timstamp of the recorded sensor reading
   * @return A long value representing the bucket that the reading for a sensor with the given timestamp should be added to
   */
  def getBucket(timestampMs: Long): Long

  /**
   * The total number of buckets to keep.  Any buckets older than getBucket(currentTime) - numberOfBuckets will be cleared from the histogram.
   * @return The number of buckets that should be kept
   */
  def numberOfBuckets: Long

  private type BucketHistogram = mutable.Map[Long, Double]
  private val histogram: BucketHistogram = mutable.Map.empty

  override def update(value: Double, timestampMs: Long): Unit = {
    this.synchronized {
      addToHistogram(timestampMs, value)
    }
  }

  override def getValue: Double = {
    this.synchronized {
      clearExpired()
      histogram.values.sum / numberOfBuckets
    }
  }

  private def addToHistogram(timestampMs: Long, value: Double): Unit = {
    val bucket = getBucket(timestampMs)
    val currentValue = histogram.getOrElse(bucket, 0.0)
    histogram.put(bucket, currentValue + value)
  }

  private def clearExpired(): Unit = {
    val expiredBucketCutoff = getBucket(Instant.now.toEpochMilli) - numberOfBuckets
    histogram.keys.foreach { bucketTime =>
      if (bucketTime < expiredBucketCutoff) {
        histogram.remove(bucketTime)
      }
    }
  }
}

trait PerSecondHistogramBucketStrategy {
  def getBucket(timestampMs: Long): Long = Instant.ofEpochMilli(timestampMs).getEpochSecond
}
