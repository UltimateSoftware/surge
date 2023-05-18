// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.metrics

trait Gauge {
  def set(value: Double): Unit
}

private[metrics] class GaugeImpl(sensor: Sensor) extends Gauge {
  override def set(value: Double): Unit = sensor.record(value)
}
