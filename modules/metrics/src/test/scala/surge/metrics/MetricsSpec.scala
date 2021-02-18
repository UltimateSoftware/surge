// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.metrics.statistics.{ ExponentiallyWeightedMovingAverage, Max, Min }

class MetricsSpec extends AnyWordSpec with Matchers {
  "Metrics" should {
    "Have a working example" in {
      val metrics = Metrics(MetricsConfig(RecordingLevel.Info))
      val sensor = metrics.sensor("some-cool-sensor")
      val sensorMax = MetricInfo("cool-sensor-max", "The maximum value we've seen for this sensor")
      val sensorMin = MetricInfo("cool-sensor-min", "The minimum value we've seen for this sensor")
      val sensorEWMA = MetricInfo("cool-sensor-ewma", "Exponentially weighted moving average for this sensor")

      // Attach some metrics to the sensor
      sensor.addMetric(sensorMax, new Max)
      sensor.addMetric(sensorMin, new Min)
      sensor.addMetric(sensorEWMA, new ExponentiallyWeightedMovingAverage(0.95))

      // Record values to the sensor
      sensor.record(100.0)
      sensor.record(10.0)

      MetricsSpecLike.metricValue(metrics, "cool-sensor-max") shouldEqual 100.0
      MetricsSpecLike.metricValue(metrics, "cool-sensor-min") shouldEqual 10.0
      MetricsSpecLike.metricValue(metrics, "cool-sensor-ewma") shouldEqual 95.5
    }

    "Properly get metric descriptions" in {
      val metrics = Metrics(MetricsConfig(RecordingLevel.Info))
      val sensor = metrics.sensor("some-cool-sensor")
      val sensorMax = MetricInfo("cool-sensor-max", "The maximum value we've seen for this sensor")
      sensor.addMetric(sensorMax, new Max)

      metrics.metricDescriptions should have length 1
      metrics.metricDescriptions.head shouldEqual MetricDescription(sensorMax.name, sensorMax.description, Map.empty, RecordingLevel.Info)
    }

    "Properly get metric values" in {
      val metrics = Metrics(MetricsConfig(RecordingLevel.Info))
      val sensor = metrics.sensor("some-cool-sensor")
      val sensorMax = MetricInfo("cool-sensor-max", "The maximum value we've seen for this sensor")
      sensor.addMetric(sensorMax, new Max)
      sensor.record(10.0)

      metrics.metricValues should have length 1
      metrics.metricValues.head shouldEqual MetricValue(sensorMax.name, Map.empty, 10.0)
    }

    "Reuse the same underlying metric instance so it can be queried directly later" in {
      val metrics = Metrics(MetricsConfig(RecordingLevel.Info))
      val sensor = metrics.sensor("some-cool-sensor")
      val sensorMax = MetricInfo("cool-sensor-max", "The maximum value we've seen for this sensor")
      sensor.addMetric(sensorMax, new Max)
      sensor.record(10.0)

      metrics.getMetrics should have length 1
      val metric = metrics.getMetrics.head
      metric.getValue shouldEqual 10.0

      sensor.record(20.0)
      metric.getValue shouldEqual 20.0
    }

    "Return an existing sensor instead of creating a new one if one already exists" in {
      val metrics = Metrics(MetricsConfig(RecordingLevel.Info))
      val sensor = metrics.sensor("some-cool-sensor")
      val duplicateSensor = metrics.sensor("some-cool-sensor")
      duplicateSensor shouldEqual sensor
    }

    "Not allow duplicate metrics to be registered" in {
      val testMetric = Metric(MetricInfo("test", "test"), RecordingLevel.Info, new Max, MetricsConfig.fromConfig)
      val metrics = new Metrics(MetricsConfig.fromConfig)

      metrics.registerMetric(testMetric)
      an[IllegalArgumentException] should be thrownBy (metrics.registerMetric(testMetric))
    }

    "Print an html table of metrics" in {
      val metrics = Metrics(MetricsConfig(RecordingLevel.Info))
      val sensor = metrics.sensor("some-cool-sensor")
      val sensorMax = MetricInfo("cool-sensor-max", "The maximum value we've seen for this sensor")
      sensor.addMetric(sensorMax, new Max)
      sensor.record(10.0)

      metrics.metricHtml should include(sensorMax.name)
      metrics.metricHtml should include(sensorMax.description)
    }
  }
}
