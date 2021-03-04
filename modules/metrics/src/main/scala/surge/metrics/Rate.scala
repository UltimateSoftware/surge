// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.time.Instant

trait Rate {
  def mark(): Unit = mark(1)
  def mark(times: Int): Unit = mark(times, Instant.now.toEpochMilli)
  def mark(times: Int, timestampMs: Long): Unit
}

private[metrics] class RateImpl(sensor: Sensor) extends Rate {
  override def mark(times: Int, timestampMs: Long): Unit = sensor.record(times, timestampMs)
}
