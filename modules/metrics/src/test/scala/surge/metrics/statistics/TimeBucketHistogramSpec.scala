// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

import java.time.Instant

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TimeBucketHistogramSpec extends AnyWordSpec with Matchers {
  private def createTestHistogram: TimeBucketHistogram = new TimeBucketHistogram {
    override def getBucket(timestampMs: Long): Long = Instant.ofEpochMilli(timestampMs).getEpochSecond
    override def numberOfBuckets: Long = 10
  }
  private def secondsAgo(seconds: Int): Long = Instant.now.minusSeconds(seconds).toEpochMilli
  "TimeBucketHistogram" should {
    "Return 0 when there are no recorded values" in {
      val testHistogram = createTestHistogram
      testHistogram.getValue shouldEqual 0.0
    }

    "Average across all buckets" in {
      val testHistogram = createTestHistogram
      testHistogram.update(5.0, secondsAgo(2))
      testHistogram.update(5.0, secondsAgo(2))
      testHistogram.update(0.0, secondsAgo(3))
      testHistogram.update(10.0, secondsAgo(4))
      testHistogram.getValue shouldEqual 2.0
    }

    "Expire buckets older than the bucketExpiration" in {
      val testHistogram = createTestHistogram
      testHistogram.update(100.0, secondsAgo(11))
      testHistogram.getValue shouldEqual 0.0
    }
  }
}
