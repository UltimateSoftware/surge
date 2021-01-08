// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors

import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future }

private object TimerExecutionContext {
  private val timerThreadPoolSize: Int = 16
  val context: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(timerThreadPoolSize))
}

trait Timer extends MetricContainerTrait[Timer] {
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

  // TODO maybe we want to track other stuff for this too?
  def toMetrics(implicit ec: ExecutionContext): Future[Seq[Metric]] = {
    value.map { timerValue =>
      Seq(Metric(name = name, value = timerValue, tenantId = tenantId))
    }
  }
}

private[metrics] class NoOpTimer(override val name: String) extends Timer {
  override def recordTime(tookMillis: Long): Unit = {}
  override def value(implicit ec: ExecutionContext): Future[Double] = Future.successful(0.0)
  override def tenantId: Option[UUID] = None
}
