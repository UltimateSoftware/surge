// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.metrics

import com.typesafe.config.ConfigFactory

import java.util.function.Supplier
import org.apache.kafka.common.MetricName
import org.slf4j.LoggerFactory
import surge.metrics.statistics.{ Count, ExponentiallyWeightedMovingAverage, MostRecentValue, RateHistogram }

import scala.collection.mutable
import scala.concurrent.duration._

object Metrics {
  lazy val globalMetricRegistry: Metrics = Metrics(config = MetricsConfig.fromConfig(ConfigFactory.load()))
}

object KafkaMetricListener {
  type KafkaMetricSupplier = Supplier[java.util.Map[MetricName, _ <: org.apache.kafka.common.Metric]]
}
trait KafkaMetricListener {
  def onMetricsRegistered(name: String, supplier: KafkaMetricListener.KafkaMetricSupplier): Unit
  def onMetricsUnregistered(name: String): Unit
}

final case class Metrics(config: MetricsConfig) {
  private val log = LoggerFactory.getLogger(getClass)

  private val sensors: mutable.Map[String, Sensor] = mutable.Map.empty
  private val metrics: mutable.Map[MetricInfo, Metric] = mutable.Map.empty
  private val kafkaMetrics: mutable.Map[String, KafkaMetricListener.KafkaMetricSupplier] = mutable.Map.empty
  private val metricListeners: mutable.Set[KafkaMetricListener] = mutable.Set.empty

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

    val oneMinuteAverage =
      metricInfo.copy(name = s"${metricInfo.name}.one-minute-average", description = s"${metricInfo.description} averaged over the past minute")
    rateSensor.addMetric(oneMinuteAverage, new RateHistogram(1.minute.toSeconds), recordingLevel)

    val fiveMinuteAverage =
      metricInfo.copy(name = s"${metricInfo.name}.five-minute-average", description = s"${metricInfo.description} averaged over the past 5 minutes")
    rateSensor.addMetric(fiveMinuteAverage, new RateHistogram(5.minutes.toSeconds), recordingLevel)

    val fifteenMinuteAverage =
      metricInfo.copy(name = s"${metricInfo.name}.fifteen-minute-average", description = s"${metricInfo.description} averaged over the past 15 minutes")
    rateSensor.addMetric(fifteenMinuteAverage, new RateHistogram(15.minutes.toSeconds), recordingLevel)

    val count =
      metricInfo.copy(name = s"${metricInfo.name}.count", description = s"${metricInfo.description}")
    rateSensor.addMetric(count, new Count, recordingLevel)

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

  def registerKafkaMetrics(name: String, metricSupplier: KafkaMetricListener.KafkaMetricSupplier): Unit = {
    if (!kafkaMetrics.contains(name)) {
      metricListeners.foreach(_.onMetricsRegistered(name, metricSupplier))
      kafkaMetrics.put(name, metricSupplier)
    }
  }

  def unregisterKafkaMetric(name: String): Unit = {
    metricListeners.foreach(_.onMetricsUnregistered(name))
    kafkaMetrics.remove(name)
  }

  def addKafkaMetricListener(listener: KafkaMetricListener): Unit = {
    if (!metricListeners.contains(listener)) {
      metricListeners.add(listener)
      kafkaMetrics.foreach { case (name, supplier) => listener.onMetricsRegistered(name, supplier) }
    }
  }

  def removeKafkaMetricListener(listener: KafkaMetricListener): Unit = {
    metricListeners.remove(listener)
  }

  def getMetrics: Vector[Metric] = metrics.values.toVector

  def metricDescriptions: Seq[MetricDescription] = {
    getMetrics.map(_.describe)
  }

  def metricValues: Seq[MetricValue] = {
    getMetrics.map(_.toMetricValue)
  }

  private def metricRowHtml(metric: Metric): String = {
    s"""
       |<tr>
       |    <td>${metric.info.name}</td>
       |    <td>${metric.info.description}</td>
       |    <td>[${metric.info.tags.mkString(", ")}]</td>
       |    <td>${metric.getValue}</td>
       |</tr>
       |""".stripMargin
  }

  def metricHtml: String = {
    s"""
       |<html>
       |<head>
       |<style>
       |table {
       |  font-family: arial, sans-serif;
       |  border-collapse: collapse;
       |  width: 100%;
       |}
       |
       |td, th {
       |  border: 1px solid #dddddd;
       |  text-align: left;
       |  padding: 8px;
       |}
       |
       |tr:nth-child(even) {
       |  background-color: #dddddd;
       |}
       |</style>
       |</head>
       |<body>
       |
       |<h2>Surge Metrics</h2>
       |
       |<table>
       |  <tr>
       |    <th>Metric Name</th>
       |    <th>Metric Description</th>
       |    <th>Metric Tags</th>
       |    <th>Metric Value</th>
       |  </tr>
       |  ${metrics.values.map(metricRowHtml).mkString("\n")}
       |</table>
       |
       |</body>
       |</html>
       |
       |""".stripMargin
  }
}
