// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

trait Counter {
  def increment(count: Int): Unit
  def decrement(count: Int): Unit

  def increment(): Unit = increment(1)
  def decrement(): Unit = decrement(1)
}

private[metrics] class CounterImpl(sensor: Sensor) extends Counter {
  override def increment(count: Int): Unit = sensor.record(count)
  override def decrement(count: Int): Unit = sensor.record(count * -1)
}
