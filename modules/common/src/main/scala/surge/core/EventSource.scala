// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.NotUsed
import akka.kafka.ConsumerSettings
import akka.stream.scaladsl.Flow
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, Deserializer, StringDeserializer }
import org.slf4j.LoggerFactory
import surge.akka.streams.graph.EitherFlow
import surge.metrics.{ MetricsProvider, NoOpMetricsProvider, Timer }

import scala.util.{ Failure, Success, Try }

trait EventSourceDeserialization[Event] {
  private val log = LoggerFactory.getLogger(getClass)
  def formatting: SurgeEventReadFormatting[Event]
  def metricsProvider: MetricsProvider = NoOpMetricsProvider
  def baseEventName: String = "SurgeDefaultBaseEventName"

  protected lazy val eventDeserializationTimer: Timer = metricsProvider.createTimer(s"${baseEventName}DeserializationTimer")
  protected lazy val eventHandlingTimer: Timer = metricsProvider.createTimer(s"${baseEventName}HandlingTimer")

  protected def onFailure(key: String, value: Array[Byte], exception: Throwable): Unit = {
    log.error("Unable to read event from byte array", exception)
  }

  protected def dataHandler(eventHandler: EventHandler[Event]): DataHandler[String, Array[Byte]] = {
    new DataHandler[String, Array[Byte]] {
      override def dataHandler[Meta]: Flow[EventPlusStreamMeta[String, Array[Byte], Meta], Meta, NotUsed] = {
        Flow[EventPlusStreamMeta[String, Array[Byte], Meta]].map { eventPlusOffset =>
          val key = eventPlusOffset.messageKey
          Option(eventPlusOffset.messageBody) match {
            case Some(value) =>
              Try(eventDeserializationTimer.time(formatting.readEvent(value))) match {
                case Failure(exception) =>
                  onFailure(key, value, exception)
                  Left(eventPlusOffset.streamMeta)
                case Success(event) =>
                  Right(EventPlusStreamMeta(key, event, eventPlusOffset.streamMeta, eventPlusOffset.headers))
              }
            case _ =>
              eventHandler.nullEventFactory(key, eventPlusOffset.headers).map { event =>
                Right(EventPlusStreamMeta(key, event, eventPlusOffset.streamMeta, eventPlusOffset.headers))
              }.getOrElse(Left(eventPlusOffset.streamMeta))
          }
        }.via(EitherFlow(
          rightFlow = eventHandler.eventHandler,
          leftFlow = Flow[Meta].map(identity)))
      }
    }
  }
}

trait EventSource[Event] extends KafkaDataSource[String, Array[Byte]] with EventSourceDeserialization[Event] {
  override val keyDeserializer: Deserializer[String] = new StringDeserializer()
  override val valueDeserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer()

  def to(sink: EventHandler[Event], consumerGroup: String): DataPipeline = {
    super.to(dataHandler(sink), consumerGroup)
  }

  private[core] def to(consumerSettings: ConsumerSettings[String, Array[Byte]])(sink: EventHandler[Event]): DataPipeline = {
    super.to(consumerSettings)(dataHandler(sink))
  }
}
