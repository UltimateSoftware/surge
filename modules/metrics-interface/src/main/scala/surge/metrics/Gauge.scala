// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.util.UUID

import scala.concurrent.{ ExecutionContext, Future }

trait Gauge extends MetricContainerTrait[Gauge] {
  def set(value: Double): Unit
  def toMetrics(implicit ec: ExecutionContext): Future[Seq[Metric]] = {
    value.map { gaugeValue ⇒
      Seq(Metric(name = name, value = gaugeValue, tenantId = tenantId))
    }
  }
}

private[metrics] class NoOpGauge(override val name: String) extends Gauge {
  override def set(value: Double): Unit = {}
  override def value(implicit ec: ExecutionContext): Future[Double] = Future.successful(0.0)
  override def tenantId: Option[UUID] = None
}
