// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.time.Instant

class RateSpec extends MetricsSpecLike {
  "Rate" should {
    "Properly track 1 minute, 5 minute, and 15 minute average rates" in {
      val testRateName = "test-rate"
      val rate = metrics.rate(MetricInfo(testRateName, "Test counter description"))

      rate.mark()
      rate.mark(1000)
      rate.mark(1000, Instant.now.minusSeconds(16 * 60).toEpochMilli) // 16 minute old reading should be ignored

      def expectedValue(numMinutes: Int): Double = {
        val numSeconds = numMinutes * 60
        1001.0 / numSeconds
      }
      metricValue(s"$testRateName.oneMinuteAverage") shouldEqual expectedValue(1)
      metricValue(s"$testRateName.fiveMinuteAverage") shouldEqual expectedValue(5)
      metricValue(s"$testRateName.fifteenMinuteAverage") shouldEqual expectedValue(15)
    }
  }
}
