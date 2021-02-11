// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import org.slf4j.LoggerFactory
import surge.metrics.statistics.{ Count, ExponentiallyWeightedMovingAverage, MostRecentValue, RateHistogram }

import scala.collection.mutable
import scala.concurrent.duration._

object Metrics {
  lazy val globalMetricRegistry: Metrics = Metrics(config = MetricsConfig.fromConfig)
}

final case class Metrics(config: MetricsConfig) {
  private val log = LoggerFactory.getLogger(getClass)

  private val sensors: mutable.Map[String, Sensor] = mutable.Map.empty
  private val metrics: mutable.Map[MetricInfo, Metric] = mutable.Map.empty

  def counter(metricInfo: MetricInfo, recordingLevel: RecordingLevel = RecordingLevel.Info): Counter = {
    val counterSensor = sensor(metricInfo.name, recordingLevel)
    counterSensor.addMetric(metricInfo, new Count, recordingLevel)
    new CounterImpl(counterSensor)
  }

  def gauge(metricInfo: MetricInfo, recordingLevel: RecordingLevel = RecordingLevel.Info): Gauge = {
    val gaugeSensor = sensor(metricInfo.name, recordingLevel)
    gaugeSensor.addMetric(metricInfo, new MostRecentValue, recordingLevel)
    new GaugeImpl(gaugeSensor)
  }

  def timer(metricInfo: MetricInfo, recordingLevel: RecordingLevel = RecordingLevel.Info): Timer = {
    val timerSensor = sensor(metricInfo.name, recordingLevel)
    timerSensor.addMetric(metricInfo, new ExponentiallyWeightedMovingAverage(0.95), recordingLevel)
    new TimerImpl(timerSensor)
  }

  def rate(metricInfo: MetricInfo, recordingLevel: RecordingLevel = RecordingLevel.Info): Rate = {
    val rateSensor = sensor(metricInfo.name, recordingLevel)

    val oneMinuteAverage = metricInfo.copy(
      name = s"${metricInfo.name}.oneMinuteAverage",
      description = s"${metricInfo.description} averaged over the past minute")
    rateSensor.addMetric(oneMinuteAverage, new RateHistogram(1.minute.toSeconds), recordingLevel)

    val fiveMinuteAverage = metricInfo.copy(
      name = s"${metricInfo.name}.fiveMinuteAverage",
      description = s"${metricInfo.description} averaged over the past 5 minutes")
    rateSensor.addMetric(fiveMinuteAverage, new RateHistogram(5.minutes.toSeconds), recordingLevel)

    val fifteenMinuteAverage = metricInfo.copy(
      name = s"${metricInfo.name}.fifteenMinuteAverage",
      description = s"${metricInfo.description} averaged over the past 15 minutes")
    rateSensor.addMetric(fifteenMinuteAverage, new RateHistogram(15.minutes.toSeconds), recordingLevel)
    new RateImpl(rateSensor)
  }

  def sensor(name: String, recordingLevel: RecordingLevel = RecordingLevel.Info): Sensor = {
    this.synchronized {
      sensors.get(name) match {
        case Some(existing) => existing
        case _ =>
          val newSensor = new Sensor(this, name, recordingLevel, config)
          sensors.put(name, newSensor)
          log.trace("Added a sensor named {}", name)
          newSensor
      }
    }
  }

  def registerMetric(metric: Metric): Unit = {
    val metricInfo = metric.info
    this.synchronized {
      if (metrics.contains(metricInfo)) {
        throw new IllegalArgumentException(s"Cannot register metric named '$metricInfo' since one with the same name already exists")
      }
      metrics.put(metricInfo, metric)
    }
  }

  def getMetrics: Seq[Metric] = metrics.values.toVector

  def metricDescriptions: Seq[MetricDescription] = {
    getMetrics.map(_.describe)
  }

  def metricValues: Seq[MetricValue] = {
    getMetrics.map(_.toMetricValue)
  }
}
