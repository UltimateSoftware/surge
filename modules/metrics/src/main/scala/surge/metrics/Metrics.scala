// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.metrics

import com.typesafe.config.ConfigFactory

import java.util.function.Supplier
import org.apache.kafka.common.MetricName
import org.slf4j.LoggerFactory
import surge.metrics.statistics.{ Count, ExponentiallyWeightedMovingAverage, MostRecentValue, RateHistogram }

import scala.collection.mutable
import scala.concurrent.duration._

import scala.jdk.CollectionConverters._

object Metrics {
  lazy val globalMetricRegistry: Metrics = Metrics(config = MetricsConfig.fromConfig(ConfigFactory.load()))

  def surgeMetricInfo: Map[String, MetricInfo] = Map(
    SURGE_AGGREGATE_AGGREGATE_STATE_SERIALIZATION_TIMER.name -> SURGE_AGGREGATE_AGGREGATE_STATE_SERIALIZATION_TIMER,
    SURGE_STATE_STORE_GET_AGGREGATE_STATE_TIMER.name -> SURGE_STATE_STORE_GET_AGGREGATE_STATE_TIMER,
    SURGE_AGGREGATE_IS_CURRENT_TIMER.name -> SURGE_AGGREGATE_IS_CURRENT_TIMER,
    SURGE_AGGREGATE_EVENT_SERIALIZATION_TIMER.name -> SURGE_AGGREGATE_EVENT_SERIALIZATION_TIMER,
    SURGE_AGGREGATE_STATE_CURRENT_RATE.name -> SURGE_AGGREGATE_STATE_CURRENT_RATE,
    SURGE_AGGREGATE_STATE_NOT_CURRENT_RATE.name -> SURGE_AGGREGATE_STATE_NOT_CURRENT_RATE,
    SURGE_AGGREGATE_KAFKA_WRITE_TIMER.name -> SURGE_AGGREGATE_KAFKA_WRITE_TIMER,
    SURGE_AGGREGATE_MESSAGE_PUBLISH_RATE.name -> SURGE_AGGREGATE_MESSAGE_PUBLISH_RATE,
    SURGE_AGGREGATE_ACTOR_STATE_INITIALIZATION_TIMER.name -> SURGE_AGGREGATE_ACTOR_STATE_INITIALIZATION_TIMER,
    SURGE_AGGREGATE_STATE_DESERIALIZATION_TIMER.name -> SURGE_AGGREGATE_STATE_DESERIALIZATION_TIMER,
    SURGE_AGGREGATE_COMMAND_HANDLING_TIMER.name -> SURGE_AGGREGATE_COMMAND_HANDLING_TIMER,
    SURGE_AGGREGATE_MESSAGE_HANDLING_TIMER.name -> SURGE_AGGREGATE_MESSAGE_HANDLING_TIMER,
    SURGE_AGGREGATE_EVENT_HANDLING_TIMER.name -> SURGE_AGGREGATE_EVENT_HANDLING_TIMER,
    SURGE_AGGREGATE_EVENT_PUBLISH_TIMER.name -> SURGE_AGGREGATE_EVENT_PUBLISH_TIMER,
    SURGE_GRPC_PROCESS_COMMAND_TIMER.name -> SURGE_GRPC_PROCESS_COMMAND_TIMER,
    SURGE_GRPC_HANDLE_EVENTS_TIMER.name -> SURGE_GRPC_HANDLE_EVENTS_TIMER,
    SURGE_GRPC_FORWARD_COMMAND_TIMER.name -> SURGE_GRPC_FORWARD_COMMAND_TIMER,
    SURGE_GRPC_GET_AGGREGATE_STATE_TIMER.name -> SURGE_GRPC_GET_AGGREGATE_STATE_TIMER,
    SURGE_EVENT_DESERIALIZATION_TIMER.name -> SURGE_EVENT_DESERIALIZATION_TIMER,
    SURGE_EVENT_SHOULD_PARSE_MESSAGE_TIMER.name -> SURGE_EVENT_SHOULD_PARSE_MESSAGE_TIMER,
    SURGE_EVENT_HANDLER_EXCEPTION_RATE.name -> SURGE_EVENT_HANDLER_EXCEPTION_RATE
  )

  val SURGE_EVENT_HANDLER_EXCEPTION_RATE: MetricInfo =
    MetricInfo(name = "surge.event.handler.exception.rate", description = "Rate of exceptions caught while handling an event")
  val SURGE_STATE_STORE_GET_AGGREGATE_STATE_TIMER: MetricInfo = MetricInfo(
    name = s"surge.state-store.get-aggregate-state-timer",
    description = "The time taken to fetch aggregate state from the KTable")

  val SURGE_AGGREGATE_IS_CURRENT_TIMER: MetricInfo = MetricInfo(
    s"surge.aggregate.is-current-timer",
    "Average time in milliseconds taken to check if a particular aggregate is up to date in the KTable")

  val SURGE_AGGREGATE_EVENT_SERIALIZATION_TIMER: MetricInfo = MetricInfo(
    name = s"surge.aggregate.event-serialization-timer",
    description = "Average time taken in milliseconds to serialize an individual event to bytes before persisting to Kafka")

  val SURGE_AGGREGATE_AGGREGATE_STATE_SERIALIZATION_TIMER: MetricInfo = MetricInfo(
    name = s"surge.aggregate.aggregate-state-serialization-timer",
    description = "Average time taken in milliseconds to serialize a new aggregate state to bytes before persisting to Kafka")

  val SURGE_AGGREGATE_STATE_CURRENT_RATE: MetricInfo = MetricInfo(
    name = s"surge.aggregate.state-current-rate",
    description = "The per-second rate of aggregates that are up-to-date in and can be loaded immediately from the KTable")

  val SURGE_AGGREGATE_STATE_NOT_CURRENT_RATE: MetricInfo = MetricInfo(
    name = s"surge.aggregate.state-not-current-rate",
    description = "The per-second rate of aggregates that are not up-to-date in the KTable and must wait to be loaded")

  val SURGE_AGGREGATE_KAFKA_WRITE_TIMER: MetricInfo = MetricInfo(
    s"surge.aggregate.kafka-write-timer",
    "Average time in milliseconds that it takes the publisher to write a batch of messages (events & state) to Kafka")

  val SURGE_AGGREGATE_MESSAGE_PUBLISH_RATE: MetricInfo = MetricInfo(
    name = s"surge.aggregate.message-publish-rate",
    description = "The per-second rate at which this aggregate attempts to publish messages to Kafka")

  val SURGE_AGGREGATE_ACTOR_STATE_INITIALIZATION_TIMER: MetricInfo = MetricInfo(
    name = s"surge.aggregate.actor-state-initialization-timer",
    description = "Average time in milliseconds taken to load aggregate state from the KTable")

  val SURGE_AGGREGATE_STATE_DESERIALIZATION_TIMER: MetricInfo = MetricInfo(
    name = s"surge.aggregate.state-deserialization-timer",
    description = "Average time taken in milliseconds to deserialize aggregate state after the bytes are read from the KTable")

  val SURGE_EVENT_DESERIALIZATION_TIMER: MetricInfo = MetricInfo(
    name = s"surge.event.deserialization-timer",
    description = "The average time (in ms) taken to deserialize events")

  val SURGE_EVENT_SHOULD_PARSE_MESSAGE_TIMER: MetricInfo = MetricInfo(
    name = s"surge.event.should-parse-message-timer",
    description = "The average time (in ms) taken by the shouldParseMessage function")

  val SURGE_AGGREGATE_COMMAND_HANDLING_TIMER: MetricInfo = MetricInfo(
    name = s"surge.aggregate.command-handling-timer",
    description = "Average time taken in milliseconds for the business logic 'processCommand' function to process a command")

  val SURGE_AGGREGATE_MESSAGE_HANDLING_TIMER: MetricInfo = MetricInfo(
    name = s"surge.aggregate.message-handling-timer",
    description = "Average time taken in milliseconds for the business logic 'processCommand' function to process a command")

  val SURGE_AGGREGATE_EVENT_HANDLING_TIMER: MetricInfo = MetricInfo(
    name = s"surge.aggregate.event-handling-timer",
    description = "Average time taken in milliseconds for the business logic 'handleEvent' function to handle an event")

  val SURGE_AGGREGATE_EVENT_PUBLISH_TIMER: MetricInfo = MetricInfo(
    name = s"surge.aggregate.event-publish-timer",
    description = "Average time taken in milliseconds to persist all generated events plus an updated state to Kafka")

  val SURGE_GRPC_PROCESS_COMMAND_TIMER: MetricInfo =
    MetricInfo("surge.grpc.process-command-timer", "The time taken by the Surge gRPC processCommand business logic callback")

  val SURGE_GRPC_HANDLE_EVENTS_TIMER: MetricInfo =
    MetricInfo("surge.grpc.handle-events-timer", "The time taken by gRPC handleEvents business logic to handle the event")

  val SURGE_GRPC_FORWARD_COMMAND_TIMER: MetricInfo =
    MetricInfo("surge.grpc.forward-command-timer", "The time taken by gRPC forwardCommand to forward the command to the aggregate")

  val SURGE_GRPC_GET_AGGREGATE_STATE_TIMER: MetricInfo =
    MetricInfo("surge.grpc.get-aggregate-state-timer", "The time taken by gRPC getState to get the state of the aggregate")
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
