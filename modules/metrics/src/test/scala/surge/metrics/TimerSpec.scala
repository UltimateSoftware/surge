// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics

import org.scalatest.concurrent.ScalaFutures

import java.util.concurrent.Executors
import scala.concurrent.{ ExecutionContext, Future }

class TimerSpec extends MetricsSpecLike with ScalaFutures {
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

    "Properly time scala Future completion time" ignore {
      val testTimerName = "future-timer-test"
      val timer = metrics.timer(MetricInfo(testTimerName, "Test timer description"))
      timer.time(Future { Thread.sleep(5L) }(ExecutionContext.global)).futureValue

      metricValue(testTimerName) should be >= 5.0
      // TODO This timer can be off by a lot, I've seen almost to 200ms. Setting to 500 for now, but may be
      //  worth seeing if it's an issue with this test or the timer utility in general
      metricValue(testTimerName) should be <= 500.0
    }

    "Properly time method completion time" ignore {
      val testTimerName = "method-timer-test"
      val timer = metrics.timer(MetricInfo(testTimerName, "Test timer description"))
      timer.time { Thread.sleep(10L) }
      metricValue(testTimerName) shouldEqual 10.0 +- 4 // Give it a little wiggle room
    }
  }
}
