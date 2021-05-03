// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics

class CounterSpec extends MetricsSpecLike {
  "Counter" should {
    "Properly increment and decrement" in {
      val testCounterName = "increment-decrement-test-counter"
      val counter = metrics.counter(MetricInfo(testCounterName, "Test counter description"))
      counter.increment()
      metricValue(testCounterName) shouldEqual 1.0
      counter.increment(5)
      metricValue(testCounterName) shouldEqual 6.0

      counter.decrement()
      metricValue(testCounterName) shouldEqual 5.0
      counter.decrement(3)
      metricValue(testCounterName) shouldEqual 2.0
    }
  }
}
