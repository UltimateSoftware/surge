// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.time.Instant

import surge.metrics.statistics.{ Max, Min }

class SensorSpec extends MetricsSpecLike {
  "Sensor" should {
    "Not register a duplicate metric" in {
      val testMetricName = "duplicate-metric-test"
      val sensor = metrics.sensor(testMetricName)
      val metricInfo = MetricInfo(testMetricName, "Test metric")
      sensor.addMetric(metricInfo, new Max)
      sensor.addMetric(metricInfo, new Min)
      sensor.record(1.0)
      sensor.record(2.0)
      metricValue(testMetricName) shouldEqual 2.0
    }

    "Not record readings if the sensor recording level is lower than the configured recording level" in {
      val testMetricName = "debug-sensor"
      val debugSensor = metrics.sensor(testMetricName, RecordingLevel.Debug)
      val metricInfo = MetricInfo(testMetricName, "Test metric")
      debugSensor.addMetric(metricInfo, new Max)
      debugSensor.record(1.0)
      debugSensor.record(2.0, Instant.now.toEpochMilli)
      metricValue(testMetricName) shouldEqual 0.0
    }

    "Emit recordings to all attached metrics" in {
      val maxMetricName = "multi-metric-max-test"
      val minMetricName = "multi-metric-min-test"
      val sensor = metrics.sensor("multi-metric-sensor")
      sensor.addMetric(MetricInfo(maxMetricName, "Test metric"), new Max)
      sensor.addMetric(MetricInfo(minMetricName, "Test metric"), new Min)
      sensor.record(1.0)
      sensor.record(2.0, Instant.now.toEpochMilli)
      metricValue(maxMetricName) shouldEqual 2.0
      metricValue(minMetricName) shouldEqual 1.0
    }
  }
}
