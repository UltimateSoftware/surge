// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

trait MetricValueProvider {
  def update(value: Double, timestampMs: Long)
  def getValue: Double
}

/**
 * Basic Information about a metric
 *
 * @param name The name of the metric
 * @param description A human readable description of the metric
 * @param tags Additional key/value attributes to associate to the metric
 */
case class MetricInfo(name: String, description: String, tags: Map[String, String] = Map.empty)

case class MetricValue(name: String, tags: Map[String, String], value: Double)
case class MetricDescription(name: String, description: String, tags: Map[String, String], recordingLevel: RecordingLevel)

case class Metric(info: MetricInfo, level: RecordingLevel, valueProvider: MetricValueProvider, metricsConfig: MetricsConfig) {
  private def shouldRecord: Boolean = level.shouldRecord(metricsConfig)

  def record(value: Double, timestampMs: Long): Unit = {
    if (shouldRecord) {
      valueProvider.update(value, timestampMs)
    }
  }

  def describe: MetricDescription = MetricDescription(name = info.name, description = info.description, tags = info.tags, recordingLevel = level)
  def toMetricValue: MetricValue = MetricValue(name = info.name, tags = info.tags, value = getValue)

  def getValue: Double = valueProvider.getValue
}

object MetricsConfig {
  private val config = ConfigFactory.load()
  private val configRecordingLevel = RecordingLevel.fromString(config.getString("metrics.recording-level"))
  def fromConfig: MetricsConfig = MetricsConfig(recordingLevel = configRecordingLevel)
}
case class MetricsConfig(recordingLevel: RecordingLevel)

sealed trait RecordingLevel {
  def shouldRecord(config: MetricsConfig): Boolean
}

object RecordingLevel {
  private val log = LoggerFactory.getLogger(getClass)
  def fromString(level: String): RecordingLevel = {
    level.toLowerCase() match {
      case "info"  => Info
      case "debug" => Debug
      case "trace" => Trace
      case other =>
        log.warn(s"Unknown recording level [{}]. Supported values are [INFO, DEBUG, TRACE]. Defaulting to [INFO]", other)
        Info
    }
  }

  case object Info extends RecordingLevel {
    override def shouldRecord(config: MetricsConfig): Boolean = true
  }
  case object Debug extends RecordingLevel {
    override def shouldRecord(config: MetricsConfig): Boolean =
      config.recordingLevel == Trace || config.recordingLevel == Debug
  }
  case object Trace extends RecordingLevel {
    override def shouldRecord(config: MetricsConfig): Boolean = config.recordingLevel == Trace
  }
}
