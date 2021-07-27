// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.ConsumerSettings
import akka.stream.scaladsl.Flow
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer, StringDeserializer}
import org.slf4j.LoggerFactory
import surge.internal.akka.streams.graph.EitherFlow
import surge.core.SurgeEventReadFormatting
import surge.internal.tracing.OpenTelemetryInstrumentation
import surge.metrics.{MetricInfo, Metrics, Timer}

import scala.util.{Failure, Success, Try}

trait EventSourceDeserialization[Event] {
  private val log = LoggerFactory.getLogger(getClass)
  def formatting: SurgeEventReadFormatting[Event]
  def metrics: Metrics = Metrics.globalMetricRegistry
  def baseEventName: String = ""
  val openTelemetry: OpenTelemetry

  protected lazy val eventDeserializationTimer: Timer = {
    val metricTags = if (baseEventName.nonEmpty) {
      Map("eventType" -> baseEventName)
    } else {
      Map.empty[String, String]
    }
    metrics.timer(
      MetricInfo(
        name = s"surge.${baseEventName.toLowerCase()}.deserialization-timer",
        description = "The average time (in ms) taken to deserialize events",
        tags = metricTags))
  }

  def shouldParseMessage(key: String, headers: Map[String, Array[Byte]]): Boolean = true

  protected def onDeserializationFailure(key: String, value: Array[Byte], exception: Throwable): Unit = {
    log.error("Unable to read event from byte array", exception)
  }

  private def deserializeMessage[Meta]
  (eventPlusOffset: EventPlusStreamMeta[String, Array[Byte], Meta])(tracer: Tracer) = {
    import surge.internal.tracing.TracingHelper._
    val span = tracer.spanBuilder("deserialize event").setNoParent().startSpan()
    val payloadByteArray: Array[Byte] = eventPlusOffset.messageBody
    val key = eventPlusOffset.messageKey
    Try(eventDeserializationTimer.time(formatting.readEvent(payloadByteArray))) match {
      case Failure(exception) =>
        onDeserializationFailure(key, payloadByteArray, exception)
        span.error(exception)
        span.end()
        Left(eventPlusOffset.streamMeta)
      case Success(event) =>
        span.end()
        Right(EventPlusStreamMeta(key, event, eventPlusOffset.streamMeta, eventPlusOffset.headers, span))
    }
  }

  protected def dataHandler(eventHandler: EventHandler[Event]): DataHandler[String, Array[Byte]] = {
    new DataHandler[String, Array[Byte]] {
      override def dataHandler[Meta](openTelemetry: OpenTelemetry): Flow[EventPlusStreamMeta[String, Array[Byte], Meta], Meta, NotUsed] = {
        val tracer: Tracer = openTelemetry.getTracer(OpenTelemetryInstrumentation.Name, OpenTelemetryInstrumentation.Version)
        Flow[EventPlusStreamMeta[String, Array[Byte], Meta]]
          .map { eventPlusOffset: EventPlusStreamMeta[String, Array[Byte], Meta] =>
            val key = eventPlusOffset.messageKey
            Option(eventPlusOffset.messageBody) match {
              case Some(_: Array[Byte]) =>
                if (shouldParseMessage(key, eventPlusOffset.headers)) {
                  deserializeMessage(eventPlusOffset)(tracer)
                } else {
                  Left(eventPlusOffset.streamMeta)
                }
              case _ =>
                eventHandler
                  .nullEventFactory(key, eventPlusOffset.headers)
                  .map { event =>
                    Right(EventPlusStreamMeta(key, event, eventPlusOffset.streamMeta, eventPlusOffset.headers))
                  }
                  .getOrElse(Left(eventPlusOffset.streamMeta))
            }
          }
          .via(EitherFlow(rightFlow = eventHandler.eventHandler(openTelemetry), leftFlow = Flow[Meta].map(identity)))
      }
    }
  }
}

trait EventSource[Event] {
  def to(sink: EventHandler[Event], consumerGroup: String): DataPipeline
  def to(sink: EventHandler[Event], consumerGroup: String, autoStart: Boolean): DataPipeline
}

trait KafkaEventSource[Event] extends EventSource[Event] with KafkaDataSource[String, Array[Byte]] with EventSourceDeserialization[Event] {
  override val keyDeserializer: Deserializer[String] = new StringDeserializer()
  override val valueDeserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer()

  override def metrics: Metrics = super.metrics

  def to(sink: EventHandler[Event], consumerGroup: String): DataPipeline = {
    super.to(dataHandler(sink), consumerGroup)
  }

  def to(sink: EventHandler[Event], consumerGroup: String, autoStart: Boolean): DataPipeline = {
    super.to(dataHandler(sink), consumerGroup, autoStart)
  }

  private[streams] def to(consumerSettings: ConsumerSettings[String, Array[Byte]])(sink: EventHandler[Event], autoStart: Boolean): DataPipeline = {
    super.to(consumerSettings)(dataHandler(sink), autoStart)
  }
}

case class KafkaStreamMeta(topic: String, partition: Int, offset: Long, committableOffset: CommittableOffset) {
  override def toString: String = {
    s"topic=$topic, partition=$partition, offset=$offset"
  }
}
