// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.time.Instant
import java.util.concurrent.Executors

import com.typesafe.config.ConfigFactory

import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future }

private object TimerExecutionContext {
  private val config = ConfigFactory.load()
  private val timerThreadPoolSize: Int = config.getInt("metrics.timer-thread-pool-size")
  val context: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(timerThreadPoolSize))
}

trait Timer {
  def recordTime(tookMillis: Long): Unit

  def time[T](fut: Future[T]): Future[T] = {
    val startTime = Instant.now()
    fut.onComplete { _ =>
      val endTime = Instant.now()
      val tookMillis = endTime.toEpochMilli - startTime.toEpochMilli
      recordTime(tookMillis)
    }(TimerExecutionContext.context)
    fut
  }

  def time[T](block: => T): T = {
    val startTime = Instant.now()
    val result = block
    val endTime = Instant.now()
    val tookMillis = endTime.toEpochMilli - startTime.toEpochMilli
    recordTime(tookMillis)
    result
  }
}

private[metrics] class TimerImpl(sensor: Sensor) extends Timer {
  override def recordTime(tookMillis: Long): Unit = sensor.record(tookMillis.toDouble)
}
