// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics

class GaugeSpec extends MetricsSpecLike {
  "Gauge" should {
    "Properly track the most recent value" in {
      val testGaugeName = "test-gauge"
      val gauge = metrics.gauge(MetricInfo(testGaugeName, "Test counter description"))

      metricValue(testGaugeName) shouldEqual 0.0

      gauge.set(3.0)
      metricValue(testGaugeName) shouldEqual 3.0

      gauge.set(1.2)
      metricValue(testGaugeName) shouldEqual 1.2
    }
  }
}
