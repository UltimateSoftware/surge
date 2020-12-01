// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.util.UUID

import scala.concurrent.{ ExecutionContext, Future }

trait MetricsProvider {
  def getMetrics(tenantId: Option[UUID] = None)(implicit ec: ExecutionContext): Future[Seq[Metric]]
  def createCounter(name: String, tenantId: Option[UUID] = None): Counter
  def createGauge(name: String, tenantId: Option[UUID] = None): Gauge
  def createRate(name: String, tenantId: Option[UUID] = None): Rate
  def createTimer(name: String, tenantId: Option[UUID] = None): Timer
}

object NoOpMetricsProvider extends MetricsProvider {
  override def getMetrics(tenantId: Option[UUID])(implicit ec: ExecutionContext): Future[Seq[Metric]] = Future.successful(Seq.empty)
  override def createCounter(name: String, tenantId: Option[UUID]): Counter = new NoOpCounter(name)
  override def createGauge(name: String, tenantId: Option[UUID]): Gauge = new NoOpGauge(name)
  override def createRate(name: String, tenantId: Option[UUID]): Rate = new NoOpRate(name)
  override def createTimer(name: String, tenantId: Option[UUID]): Timer = new NoOpTimer(name)
}
