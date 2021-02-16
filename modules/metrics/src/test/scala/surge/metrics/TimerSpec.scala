// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import org.scalatest.concurrent.Eventually

import scala.concurrent.{ ExecutionContext, Future }

class TimerSpec extends MetricsSpecLike with Eventually {
  "Timer" should {
    "Properly track explicitly recorded time" in {
      val testTimerName = "record-time-test"
      val timer = metrics.timer(MetricInfo(testTimerName, "Test timer description"))

      metricValue(testTimerName) shouldEqual 0.0

      timer.recordTime(100L)
      metricValue(testTimerName) shouldEqual 100.0

      timer.recordTime(10L)
      metricValue(testTimerName) shouldEqual 95.5
    }

    "Properly time scala Future completion time" in {
      val testTimerName = "future-timer-test"
      val timer = metrics.timer(MetricInfo(testTimerName, "Test timer description"))
      lazy val future = Future { Thread.sleep(10L) }(ExecutionContext.global)
      timer.time(future)
      eventually {
        // The timing for the future is a little flaky and can sometimes be 10+ ms off in these tests.
        // Just assert that we're at least timing the amount of time we've slept for to prevent this test from being really flaky.
        metricValue(testTimerName) should be >= 10.0
        metricValue(testTimerName) should be <= 100.0
      }
    }

    "Properly time method completion time" in {
      val testTimerName = "method-timer-test"
      val timer = metrics.timer(MetricInfo(testTimerName, "Test timer description"))
      timer.time({ Thread.sleep(10L) })
      metricValue(testTimerName) shouldEqual 10.0 +- 4 // Give it a little wiggle room
    }
  }
}
