// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object MetricsSpecLike {
  def metricValue(metrics: Metrics, metricName: String): Double = {
    metrics.getMetrics
      .find(metric => metric.info.name == metricName)
      .getOrElse(throw new AssertionError(s"Could not find a metric named $metricName"))
      .getValue
  }
}
trait MetricsSpecLike extends AnyWordSpecLike with Matchers {
  protected val metrics = new Metrics(MetricsConfig.fromConfig)

  protected def metricValue(metricName: String): Double = {
    MetricsSpecLike.metricValue(metrics, metricName)
  }
}
