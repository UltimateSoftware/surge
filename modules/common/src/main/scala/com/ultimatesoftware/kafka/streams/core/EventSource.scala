// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.kafka.ConsumerSettings
import akka.stream.scaladsl.Flow
import akka.{ Done, NotUsed }
import com.ultimatesoftware.akka.streams.graph.{ OptionFlow, RecordTimeFlow }
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, NoOpMetricsProvider, Timer }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, Deserializer, StringDeserializer }
import org.slf4j.{ Logger, LoggerFactory }

import scala.util.{ Failure, Success, Try }

trait EventSourceDeserialization[Event, EvtMeta] {
  private val log = LoggerFactory.getLogger(getClass)
  def formatting: SurgeEventReadFormatting[Event, EvtMeta]
  def metricsProvider: MetricsProvider = NoOpMetricsProvider
  def baseEventName: String = "SurgeDefaultBaseEventName"

  protected lazy val eventDeserializationTimer: Timer = metricsProvider.createTimer(s"${baseEventName}DeserializationTimer")
  protected lazy val eventHandlingTimer: Timer = metricsProvider.createTimer(s"${baseEventName}HandlingTimer")

  protected def onFailure(key: String, value: Array[Byte], exception: Throwable): Unit = {
    log.error("Unable to read event from byte array", exception)
  }

  protected def dataHandler(eventHandler: EventHandler[Event, EvtMeta]): DataHandler[String, Array[Byte]] = {
    new DataHandler[String, Array[Byte]] {
      override def dataHandler: Flow[(String, Array[Byte]), Any, NotUsed] = {
        Flow[(String, Array[Byte])].map {
          case (key, value) ⇒
            Try(eventDeserializationTimer.time(formatting.readEvent(value))) match {
              case Failure(exception) ⇒
                onFailure(key, value, exception)
                None
              case Success(eventPlusMeta) ⇒
                val event = eventPlusMeta._1
                if (eventPlusMeta._2.isEmpty) {
                  log.error("Unable to extract event metadata from byte array")
                }
                eventPlusMeta._2.map { meta ⇒
                  event -> meta
                }
            }
        }.via(OptionFlow(
          someFlow = RecordTimeFlow(flow = eventHandler.eventHandler, timer = eventHandlingTimer),
          noneFlow = Flow[None.type].map(_ ⇒ Done)))
      }
    }
  }
}

trait EventSource[Event, EvtMeta] extends KafkaDataSource[String, Array[Byte]] with EventSourceDeserialization[Event, EvtMeta] {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  @deprecated("Used as the default consumer group when using to(sink), but to(sink, consumerGroup) should be used instead.", "0.4.11")
  def consumerGroup: String = {
    log.error(
      "Using deprecated default for consumerGroup from calling the EventSource.to(sink) method! " +
        "Business logic should use the EventSource.to(sink, consumerGroup) method to explicitly set a consumer group per data pipeline." +
        "This method may be removed in a future minor release of Surge.  The line calling this deprecated method can be found in the following stack trace:",
      new RuntimeException())

    "deprecated-default-surge-consumer-group-for-compatibility"
  }

  override val keyDeserializer: Deserializer[String] = new StringDeserializer()
  override val valueDeserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer()

  @deprecated("Use to(sink, consumerGroup) instead to avoid sharing consumer groups across different data pipelines.  " +
    "You should also remove the `consumerGroup` override once you switch to using to(sink, consumerGroup)", "0.4.11")
  def to(sink: EventHandler[Event, EvtMeta]): DataPipeline = {
    to(sink, consumerGroup)
  }

  def to(sink: EventHandler[Event, EvtMeta], consumerGroup: String): DataPipeline = {
    super.to(dataHandler(sink), consumerGroup)
  }

  private[core] def to(consumerSettings: ConsumerSettings[String, Array[Byte]])(sink: EventHandler[Event, EvtMeta]): DataPipeline = {
    super.to(consumerSettings)(dataHandler(sink))
  }
}
