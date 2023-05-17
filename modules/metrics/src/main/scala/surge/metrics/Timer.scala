// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.time.Instant
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

trait Timer {
  def recordTime(tookMillis: Long): Unit

  def timeFuture[T](body: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val startTime = Instant.now()
    val fut: Future[T] = body
    fut.onComplete { _: Try[T] =>
      val endTime = Instant.now()
      val tookMillis = endTime.toEpochMilli - startTime.toEpochMilli
      recordTime(tookMillis)
    }
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
