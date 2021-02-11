// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.time.Instant

import scala.collection.mutable

class Sensor(registry: Metrics, name: String, recordingLevel: RecordingLevel, config: MetricsConfig) {
  private val metrics: mutable.Map[MetricInfo, Metric] = mutable.Map.empty

  def shouldRecord: Boolean = recordingLevel.shouldRecord(config)

  def record(value: Double): Unit = {
    if (shouldRecord) {
      recordInternal(value, Instant.now().toEpochMilli)
    }
  }

  def record(value: Double, timestampMs: Long): Unit = {
    if (shouldRecord) {
      recordInternal(value, timestampMs)
    }
  }

  def addMetric(metricInfo: MetricInfo, valueProvider: MetricValueProvider, recordingLevel: RecordingLevel = RecordingLevel.Info): Unit = {
    this.synchronized {
      if (!metrics.contains(metricInfo)) {
        val newMetric = Metric(metricInfo, recordingLevel, valueProvider, config)
        registry.registerMetric(newMetric)
        metrics.put(metricInfo, newMetric)
      }
    }
  }

  private def recordInternal(value: Double, timestampMs: Long): Unit = {
    metrics.values.foreach(metric => metric.record(value, timestampMs))
  }
}
