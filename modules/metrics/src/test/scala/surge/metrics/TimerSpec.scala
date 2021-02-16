// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import org.scalatest.concurrent.Eventually

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._

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
      lazy val future = Future { Thread.sleep(25L) }(ExecutionContext.global)
      timer.time(future)
      eventually {
        metricValue(testTimerName) shouldEqual 25.0 +- 10 // Give it a little wiggle room

      }
    }

    "Properly time method completion time" in {
      val testTimerName = "method-timer-test"
      val timer = metrics.timer(MetricInfo(testTimerName, "Test timer description"))
      timer.time({ Thread.sleep(25L) })
      metricValue(testTimerName) shouldEqual 25.0 +- 4 // Give it a little wiggle room
    }
  }
}
