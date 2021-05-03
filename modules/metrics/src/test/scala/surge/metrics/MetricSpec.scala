// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.time.Instant

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.metrics.statistics.Max

class MetricSpec extends AnyWordSpec with Matchers {
  "Metric" should {
    "Not update if the metric recording level is lower than the registry configured recording level" in {
      val metric = Metric(MetricInfo("test", "test"), RecordingLevel.Debug, new Max, MetricsConfig(recordingLevel = RecordingLevel.Info))
      metric.record(5.0, Instant.now.toEpochMilli)
      metric.getValue shouldEqual 0.0
    }
  }

  "RecordingLevel" should {
    "Properly parse from a valid string" in {
      RecordingLevel.fromString("InFo") shouldEqual RecordingLevel.Info
      RecordingLevel.fromString("INFO") shouldEqual RecordingLevel.Info
      RecordingLevel.fromString("info") shouldEqual RecordingLevel.Info

      RecordingLevel.fromString("DeBuG") shouldEqual RecordingLevel.Debug
      RecordingLevel.fromString("DEBUG") shouldEqual RecordingLevel.Debug
      RecordingLevel.fromString("debug") shouldEqual RecordingLevel.Debug

      RecordingLevel.fromString("TrAcE") shouldEqual RecordingLevel.Trace
      RecordingLevel.fromString("TRACE") shouldEqual RecordingLevel.Trace
      RecordingLevel.fromString("trace") shouldEqual RecordingLevel.Trace
    }

    "Default to Info level when parsing from an invalid string" in {
      RecordingLevel.fromString("unknownLevel") shouldEqual RecordingLevel.Info
    }

    "RecordingLevel.Info" should {
      "Record for any configured metrics recording level" in {
        RecordingLevel.Info.shouldRecord(MetricsConfig(recordingLevel = RecordingLevel.Info)) shouldEqual true
        RecordingLevel.Info.shouldRecord(MetricsConfig(recordingLevel = RecordingLevel.Debug)) shouldEqual true
        RecordingLevel.Info.shouldRecord(MetricsConfig(recordingLevel = RecordingLevel.Trace)) shouldEqual true
      }
    }

    "RecordingLevel.Debug" should {
      "Know to only record if metrics are configured to record at debug or trace level" in {
        RecordingLevel.Debug.shouldRecord(MetricsConfig(recordingLevel = RecordingLevel.Info)) shouldEqual false
        RecordingLevel.Debug.shouldRecord(MetricsConfig(recordingLevel = RecordingLevel.Debug)) shouldEqual true
        RecordingLevel.Debug.shouldRecord(MetricsConfig(recordingLevel = RecordingLevel.Trace)) shouldEqual true
      }
    }

    "RecordingLevel.Trace" should {
      "Know to only record if metrics are configured to record at trace level" in {
        RecordingLevel.Trace.shouldRecord(MetricsConfig(recordingLevel = RecordingLevel.Info)) shouldEqual false
        RecordingLevel.Trace.shouldRecord(MetricsConfig(recordingLevel = RecordingLevel.Debug)) shouldEqual false
        RecordingLevel.Trace.shouldRecord(MetricsConfig(recordingLevel = RecordingLevel.Trace)) shouldEqual true
      }
    }
  }
}
