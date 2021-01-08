// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.util.UUID

import scala.concurrent.{ ExecutionContext, Future }

trait Counter extends MetricContainerTrait[Counter] {
  def increment(count: Int): Unit
  def decrement(count: Int): Unit

  def increment(): Unit = increment(1)
  def decrement(): Unit = decrement(1)
  def toMetrics(implicit ec: ExecutionContext): Future[Seq[Metric]] = {
    value.map { counterValue =>
      Seq(Metric(name = name, value = counterValue, tenantId = tenantId))
    }
  }
}

private[metrics] class NoOpCounter(override val name: String) extends Counter {
  override def increment(count: Int): Unit = {}
  override def decrement(count: Int): Unit = {}
  override def value(implicit ec: ExecutionContext): Future[Double] = Future.successful(0.0)
  override def tenantId: Option[UUID] = None
}
