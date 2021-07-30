// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{ Millis, Span }

import java.time.Instant
import scala.concurrent.duration._
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future }

class TimerSpec extends TestKit(ActorSystem("TimerSpec")) with MetricsSpecLike with Eventually with BeforeAndAfterAll {

  private val executor = Executors.newFixedThreadPool(1)
  private val singleThreadedEc: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

  override def afterAll(): Unit = {
    executor.shutdown()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "Timer" should {
    "properly track explicitly recorded time" in {
      val testTimerName = "record-time-test"
      val timer = metrics.timer(MetricInfo(testTimerName, "Test timer description"))

      metricValue(testTimerName) shouldEqual 0.0

      timer.recordTime(100L)
      metricValue(testTimerName) shouldEqual 100.0

      timer.recordTime(10L)
      metricValue(testTimerName) shouldEqual 95.5
    }

    "properly time scala Future completion time" in {

      import system.dispatcher
      val testTimerName = "future-timer-test"
      val timer = metrics.timer(MetricInfo(testTimerName, "Test timer description"))

      timer.timeFuture { akka.pattern.after(50.millis)(Future.successful(())) }

      val marginErr = 25.0

      eventually {
        // The timing for the future is a little flaky and can sometimes be 10+ ms off in these tests.
        // Just assert that we're at least timing the amount of time we've slept for to prevent this test from being really flaky.
        // This test has been as much as 109 ms off when run in pipeline. Increased upper bound to minimize flakiness
        metricValue(testTimerName) should be >= 50.0
        metricValue(testTimerName) should be <= 50.0 + marginErr

      }
    }

    "properly time scala Future completion time (including queue time)" in {

      val testTimerName = "my-timer"
      val timer = metrics.timer(MetricInfo(testTimerName, "Test timer description"))

      implicit val ec = singleThreadedEc

      Future { Thread.sleep(200) } // cause 200ms "scheduling time" for the next future

      timer.timeFuture {
        Future { Thread.sleep(100) }
      }

      val marginErr = 15.0

      eventually(timeout = Timeout(Span(500, Millis))) {
        metricValue(testTimerName) should be >= 300.0
        metricValue(testTimerName) should be <= 300.0 + marginErr
      }

    }

    "not cause thread starvation issues" in {
      import system.dispatcher
      val numFutures = 100
      val minSleep = 200
      val maxSleep = 500
      val expected = mutable.Map[String, Int]()
      (1 to numFutures).foreach(item => {
        val timerName = s"Future-$item"
        val timer = metrics.timer(MetricInfo(timerName, description = "just a future"))
        val sleep = scala.util.Random.between(minSleep, maxSleep)
        expected(timerName) = sleep
        timer.timeFuture {
          akka.pattern.after(sleep.millis)(Future.successful(()))
        }
      })

      val marginErr = 50.0

      eventually(timeout = Timeout(Span(1.1 * maxSleep, Millis))) {
        (1 to numFutures).foreach { item =>
          {
            val timerName = s"Future-$item"
            val expectedTime = expected(timerName).toDouble
            metricValue(timerName) should be >= expectedTime
            metricValue(timerName) should be <= expectedTime + marginErr
          }
        }
      }

    }

    "properly time method completion time" in {
      val testTimerName = "method-timer-test"
      val timer = metrics.timer(MetricInfo(testTimerName, "Test timer description"))
      timer.time { Thread.sleep(10L) }
      metricValue(testTimerName) shouldEqual 10.0 +- 4 // Give it a little wiggle room
    }
  }
}
