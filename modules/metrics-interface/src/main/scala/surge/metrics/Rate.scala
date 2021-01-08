// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.time.Instant
import java.util.UUID

import scala.concurrent.{ ExecutionContext, Future }

trait Rate extends MetricContainerTrait[Rate] {
  private val keepMinutes = 15
  private val keepSeconds = keepMinutes * 60
  private type TimeBucketHistogram = Map[Long, Long]

  protected def addRateToHistogram(epochSecond: Long, times: Int): Unit
  protected def removeRateFromHistogram(epochSecond: Long): Unit

  // Histogram of time (in epoch seconds) to number of times a rate was marked in that second
  protected def rateHistogram: Future[TimeBucketHistogram]

  def mark(): Unit = mark(1)
  def mark(times: Int): Unit = {
    val currentEpochSeconds = Instant.now.getEpochSecond
    addRateToHistogram(currentEpochSeconds, times)
  }

  override def value(implicit ec: ExecutionContext): Future[Double] = rateHistogram.map(oneMinuteRatePerSecond)
  protected def clearExpired(): Unit = {
    rateHistogram.map { hist =>
      val expirationEpochSecond = Instant.now().getEpochSecond - keepSeconds
      val expiredRates = hist.keys.filter(_ < expirationEpochSecond)
      expiredRates.foreach(epochSecond => removeRateFromHistogram(epochSecond))
    }(ExecutionContext.global)
  }

  private def oneSecondRateForRange(hist: TimeBucketHistogram, startEpochSecond: Long, endEpochSecond: Long): Double = {
    val inRangeValues = hist.filterKeys(epochSecond => epochSecond > startEpochSecond && epochSecond < endEpochSecond)
    val numberSecondsInRange = endEpochSecond - startEpochSecond

    val res = inRangeValues.values.sum.toDouble / numberSecondsInRange.toDouble

    res
  }

  private def oneMinuteRatePerSecond(hist: TimeBucketHistogram): Double = {
    val now = Instant.now().getEpochSecond
    oneSecondRateForRange(hist, startEpochSecond = now - 60, endEpochSecond = now)
  }

  private val fiveMinutes = 60 * 5
  private def fiveMinuteRatePerSecond(hist: TimeBucketHistogram): Double = {
    val now = Instant.now().getEpochSecond
    oneSecondRateForRange(hist, startEpochSecond = now - fiveMinutes, endEpochSecond = now)
  }

  private val fifteenMinutes = 60 * 15
  private def fifteenMinuteRatePerSecond(hist: TimeBucketHistogram): Double = {
    val now = Instant.now().getEpochSecond
    oneSecondRateForRange(hist, startEpochSecond = now - fifteenMinutes, endEpochSecond = now)
  }

  def toMetrics(implicit ec: ExecutionContext): Future[Seq[Metric]] = {
    rateHistogram.map { hist =>
      val oneMinute = Metric(name = s"$name.oneMinuteRatePerSecond", value = oneMinuteRatePerSecond(hist), tenantId = tenantId)
      val fiveMinute = Metric(name = s"$name.fiveMinuteRatePerSecond", value = fiveMinuteRatePerSecond(hist), tenantId = tenantId)
      val fifteenMinute = Metric(name = s"$name.fifteenMinuteRatePerSecond", value = fifteenMinuteRatePerSecond(hist), tenantId = tenantId)
      Seq(oneMinute, fiveMinute, fifteenMinute)
    }
  }
}

private[metrics] class NoOpRate(override val name: String) extends Rate {
  override protected def addRateToHistogram(epochSecond: Long, times: Int): Unit = {}
  override protected def removeRateFromHistogram(epochSecond: Long): Unit = {}
  override protected def rateHistogram: Future[Map[Long, Long]] = Future.successful(Map.empty)
  override def tenantId: Option[UUID] = None
}
