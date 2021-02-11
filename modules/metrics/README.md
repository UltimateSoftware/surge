# Surge Metrics

Surge metrics are collected internally and accessible via a metrics registry, which simply collects metrics internally and exposes them as a queryable list.

## Consuming Metrics From Surge In a Service

Most components of Surge allow for overriding a `Metrics` object.  These are defaulted to the static `Metrics.globalMetricRegistry` object but can be overridden if you want to use a different metrics configuration or different metric registries for different components.  Once you have access to a `Metrics` object that's wired into a component, the Surge internals will register any reported metrics back up to that registry so that they can all be accessed through the single interface.
To access metrics, first get a reference to a `Metrics` object that's wired into a component whose metrics you want to view (likely just by loading the `Metrics.globalMetricsRegistry` since that's what everything defaults to).  From there, the `getMetrics` function will return information about and the current value for any reported & available metrics.
```scala
import surge.metrics.Metrics

val metricRegistry = Metrics.globalMetricRegistry
val allAvailableMetrics = metricRegistry.getMetrics
```

## Adding Metrics Internally (Surge Developers)

All metrics can be created from an instance of the `Metrics` class.  While the companion object for this class has a `globalMetricRegistry` available, an instance of `Metrics` should be plumbed through to wherever you want to expose metrics from, that way an outside consumer could separate metrics into different logical metrics registries for separate reporting if they want.
The `Metrics` class has first-class support for `counters` (increment or decrement a value), `gauges` (set and report the most recent value recorded), `rates` (a per-second rate averaged for 1 minute, 5 minute, and 15 minute intervals), and `timers` (measures the timing of method calls and Scala `Future` completion).

Additionally, the `Metrics` class supports a generic `Sensor`.  A `Sensor` is a value that can be measured, for example recording each time a method is called.  A `Metric` is some insight we hope to gain from a `Sensor` - for example a count of the number of times that method is called, or a 5 minute rolling average of that same sensor for our method call.  While a sensor itself will not report metrics, it may have one or more metrics attached to it.  The purpose of this is to allow for a single place to record to a sensor reading while each metric can receive that update and report out individually.

All metrics are created with a `MetricInfo` object which includes a name, description, and optional tags associated to the metric.  By including the description to the metric where it's created, our Metrics registry can self-document as we add more metrics over time.
```scala
import surge.metrics.MetricInfo

val myCustomMetric = MetricInfo(
  name = "my-custom-metric",
  description = "Just an example metric for the docs",
  tags = Seq("metricTag", "and another one")
)
```

### Standard Metrics

#### Counters
Counters are meant to keep track of the number of times something happens.  They can be incremented or decremented but currently report a sum of the sensor readings they see.
```scala
import surge.metrics._

class SomeClass(metrics: Metrics) {
  val counter = metrics.counter(MetricInfo(name = "some-custom-counter", description = "Just an example counter"))

  counter.increment() // Increment the counter
  counter.increment(3) // Increment the counter by 3
  counter.decrement() // Decrement the counter
  counter.decrement(3) // Decrement the counter by 3
}
```

#### Rates
A rate tracks the per-second rate at which something occurs.  They are currently reported via 3 metrics - an average over the last 1 minute, 5 minutes, and 15 minutes.
```scala
import surge.metrics._

class SomeClass(metrics: Metrics) {
  val rate = metrics.rate(MetricInfo(name = "some-custom-rate", description = "Just an example rate"))  

  rate.mark() // Mark the rate one time
  rate.mark(3) // Mark the rate 3 times
}
```

#### Gauges
Gauges are meant to track a set value over time.  They currently report the most recent value recorded.
```scala
import surge.metrics._

class SomeClass(metrics: Metrics) {
  val gauge = metrics.gauge(MetricInfo(name = "some-custom-guage", description = "Just an example gauge"))

  gauge.set(1.0)
  gauge.set(5.0)
}
```

#### Timers
Timers are meant to time a particular block of code or Scala Future.  They report using an Exponentially weighted moving average.
```scala
import surge.metrics._

import scala.concurrent._

class SomeClass(metrics: Metrics) {
  val timer = metrics.timer(MetricInfo(name = "some-custom-timer", description = "Just an example timer"))

  timer.time(doSomeLengthyOperation())
  timer.time(doSomethingInAFuture())
  timer.recordTime(15L) // Record time directly if needed

  def doSomeLengthyOperation(): Unit = {
    Thread.sleep(1000L)
  }
  
  def doSomethingInAFuture(): Future[Any] = Future {
    Thread.sleep(1000L)
  }
}
```

### Sensors & Custom Metrics

You can attach any number of custom metrics to a `Sensor` plus add additional metric calculations if you need to calculate and report something not already available.

```scala
import surge.metrics._
import surge.metrics.statistics._

class SomeClass(metrics: Metrics) {
  val sensor = metrics.sensor("custom-sensor")
  val sensorMax = MetricInfo("custom-sensor-max", "Example of a max value across the lifetime of this sensor")
  
  // The valueProvider passed in is what calculates the metric
  sensor.addMetric(metricInfo = sensorMax, valueProvider = new Max)

  // You can attach multiple different metrics to a sensor if you want
  val sensorMin = MetricInfo("custom-sensor-min", "Example of a min value across the lifetime of this sensor")
  sensor.addMetric(sensorMin, new Min)

  // Record something to the sensor.  This value is reported down to all metrics attached to the sensor
  sensor.record(2.0)
}
```

If any of the currently available metric calculations available under the `surge.metrics.statistics` package don't meet your needs, you can implement an instance of `MetricValueProvider` to perform the calculation you need for your metric.

As an example:
```scala
import surge.metrics.MetricValueProvider

class Max extends MetricValueProvider {
  private var currentMax: Option[Double] = None
  // The sensor this is attached to calls update every time something is recorded to the sensor
  override def update(value: Double, timestampMs: Long): Unit = {
    currentMax = Some(Math.max(value, currentMax.getOrElse(value)))
  }

  // getValue is called every time something checks the value of a metric
  override def getValue: Double = currentMax.getOrElse(0.0)
}
```

### Metric Recording Level

In all of the interfaces where you can create a metric you have the option to additionally include a metric `RecordingLevel` for the metric (default `Info`).  The `Metrics` object registry is also configured with a `RecordingLevel` and will automatically ignore (not record to or report) any metrics whose recording level is lower than the configured recording level.  More expensive metrics can be set to `Debug` or `Trace` in order to reduce their impact when running normally, but can be easily turned on for applications that want to do performance tuning.
