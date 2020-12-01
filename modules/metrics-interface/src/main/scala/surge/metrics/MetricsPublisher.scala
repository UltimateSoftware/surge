// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

trait MetricsPublisher {
  def publish(metrics: Seq[Metric]): Unit
}

object NoOpsMetricsPublisher extends MetricsPublisher {
  def publish(metrics: Seq[Metric]): Unit = {
  }
}
