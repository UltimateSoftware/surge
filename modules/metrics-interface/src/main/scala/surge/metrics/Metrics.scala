// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.time.format.DateTimeFormatter
import java.util.UUID

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }
import java.time.{ Instant, ZoneId, ZoneOffset }

/**
 * Metrics is the core DTO representing a set of Metrics for an Application.
 * It snapshots system metrics and provides methods for updating and incrementing metrics
 *
 * Applications can not create objects of this class directly.
 * It is created by and maintained as the state of the MonitoringActor
 *
 * Applications must mixin UltiMetrics or a derivative to enable Metrics
 */
final case class Metrics(
    timestamp: Instant,
    hostname: String,
    processId: String,
    serviceName: String,
    commitHash: String,
    metrics: Seq[MetricContainer]) {

  def updateMetric(name: String, value: Double, tenantId: Option[UUID] = None): Metrics =
    replaceMetric(MetricContainer(name, value, tenantId))

  def incrementMetric(name: String, incrementBy: Double, tenantId: Option[UUID] = None): Metrics =
    metrics.find(_.name.equals(name)) match {
      case Some(metric) => replaceMetric(metric.increment(incrementBy))
      case None         => replaceMetric(MetricContainer(name, incrementBy, tenantId))
    }

  def incrementMetricWithIntervalBucket(
    name: String, intervalSize: FiniteDuration, duration: FiniteDuration,
    incrementBy: Double, tenantId: Option[UUID] = None): Metrics = {
    metrics.find(_.name.equals(name)) match {
      case Some(metric @ MetricContainer(_, _, _, Some(_))) =>
        replaceMetric(metric.increment(incrementBy))

      case Some(metric @ MetricContainer(_, _, _, _)) =>
        replaceMetric(metric.copy(intervalBucket = Some(IntervalBucket(intervalSize, duration))).increment(incrementBy))

      case _ =>
        replaceMetric(MetricContainer(name, 0d, tenantId, Some(IntervalBucket(intervalSize, duration))).increment(incrementBy))
    }
  }

  private def replaceMetric(metric: MetricContainer) =
    copy(metrics = metrics.filter(!_.name.equalsIgnoreCase(metric.name)) :+ metric)

  lazy val metricMap: Map[String, Double] = metrics.map(m => m.name -> m.value).toMap

}

trait MetricContainerTrait[T <: MetricContainerTrait[T]] {
  def name: String
  def value(implicit ec: ExecutionContext): Future[Double] // TODO does this make sense? Not every metric necessarily has a single "value"
  def tenantId: Option[UUID]

  def toMetrics(implicit ec: ExecutionContext): Future[Seq[Metric]]
}

object MetricContainer {
  val isoFormatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
}
final case class MetricContainer(name: String, value: Double, tenantId: Option[UUID] = None, intervalBucket: Option[IntervalBucket] = None) {

  def increment(incrementBy: Double): MetricContainer =
    copy(value = value + incrementBy, intervalBucket = intervalBucket.map(_.increment(incrementBy)))

  def toMetrics: Seq[Metric] = {
    intervalBucket.map { intervalBucket =>
      Seq(
        Metric(s"$name.${intervalBucket.duration.toString()}.total", intervalBucket.durationTotal, tenantId),
        Metric(s"$name.${intervalBucket.intervalSize.toString()}.average", intervalBucket.intervalAverage, tenantId),
        Metric(s"$name.${intervalBucket.intervalSize.toString()}.peak", intervalBucket.peakInterval._2, tenantId,
          Some(Map("peakInterval" -> MetricContainer.isoFormatter.format(intervalBucket.peakInterval._1)))),
        Metric(s"$name.${intervalBucket.intervalSize.toString()}.nadir", intervalBucket.nadirInterval._2, tenantId))
    }.getOrElse(Seq.empty) :+ Metric(name, value, tenantId)
  }
}

final case class IntervalBucket(
    intervalSize: FiniteDuration,
    duration: FiniteDuration,
    currentKey: Instant = Instant.now(),
    intervals: Map[Instant, Double] = Map.empty) {

  private lazy val intervalCount: Int = (duration / intervalSize).toInt
  private def dateTimeKey(dateTime: Instant): Instant =
    Instant.ofEpochMilli(dateTime.toEpochMilli - (dateTime.toEpochMilli % intervalSize.toMillis))

  def increment(incrementBy: Double = 1d): IntervalBucket = {
    def updateInterval(intervalMap: Map[Instant, Double], updateInterval: (Instant, Double)): Map[Instant, Double] = {
      (intervalMap - updateInterval._1 + updateInterval)
        // Remove any intervals that are older than the duration
        .filter(interval => interval._1.toEpochMilli > updateInterval._1.toEpochMilli - duration.toMillis + intervalSize.toMillis)
    }

    dateTimeKey(Instant.now()) match {
      case key if key == currentKey =>
        intervals.find(_._1 == key) match {
          case Some(interval) => copy(intervals = updateInterval(intervals, key -> (interval._2 + incrementBy)))
          case None           => copy(currentKey = key, intervals = updateInterval(intervals, key -> incrementBy))
        }
      case key =>
        intervals.find(_._1 == key) match {
          case Some(_) => copy(currentKey = key, intervals = updateInterval(intervals, key -> incrementBy))
          case None    => copy(intervals = updateInterval(intervals, key -> incrementBy))
        }
    }
  }

  def durationTotal: Double = intervals.values.sum
  def intervalAverage: Double = if (intervals.nonEmpty) { intervals.values.sum / intervals.size } else { 0d }
  def peakInterval: (Instant, Double) = {
    val maxValue = intervals.values.fold(0d)((a, b) => scala.math.max(a, b))
    intervals.find(_._2.equals(maxValue)).getOrElse((dateTimeKey(Instant.now()), 0d))
  }
  def nadirInterval: (Instant, Double) = {
    val minValue = intervals.values.fold(0d)((a, b) => scala.math.max(a, b))
    intervals.find(_._2.equals(minValue)).getOrElse((dateTimeKey(Instant.now()), 0d))
  }
}

