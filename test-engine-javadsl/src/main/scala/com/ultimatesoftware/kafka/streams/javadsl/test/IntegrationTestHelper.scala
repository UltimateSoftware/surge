// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl.test

import java.util.UUID
import java.util.concurrent.{ Callable, TimeUnit }

import com.ultimatesoftware.kafka.streams.javadsl.UltiKafkaStreamsCommandBusinessLogic
import com.ultimatesoftware.scala.core.domain.StatePlusMetadata
import com.ultimatesoftware.scala.core.kafka.{ KafkaBytesConsumer, KafkaBytesProducer, UltiKafkaConsumerConfig }
import com.ultimatesoftware.scala.core.messaging.{ EventMessage, EventMessageSerializer, EventMessageSerializerRegistry, EventMessageTypeInfo, EventProperties }
import com.ultimatesoftware.scala.core.utils.EmptyUUID
import com.ultimatesoftware.scala.core.utils.JsonUtils
import org.awaitility.Awaitility._
import org.awaitility.core.ThrowingRunnable
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.Format

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class IntegrationTestHelper[AggId, Agg, Cmd, Event, CmdMeta, Envelope](
    kafkaBrokers: java.util.List[String],
    eventMessageSerializers: java.util.List[EventMessageSerializer[_ <: Event]],
    val ultiBusinessLogic: UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta, Envelope]) {
  private val timeoutSeconds = 30
  private val log: Logger = LoggerFactory.getLogger(getClass)

  private implicit def byNameFunctionToCallableOfType[T](function: ⇒ T): Callable[T] = () ⇒ function
  private implicit def byNameFunctionToCallableOfBoolean(function: ⇒ scala.Boolean): Callable[java.lang.Boolean] = () ⇒ function
  private implicit def byNameFunctionToRunnable[T](function: ⇒ T): ThrowingRunnable = () ⇒ function

  private val eventMessageSerializerRegistry = EventMessageSerializerRegistry[Event](eventMessageSerializers.asScala)
  private implicit val eventFormatter: Format[EventMessage[Event]] = eventMessageSerializerRegistry.formatFromSchemaRegistry
  private val consumerGroupName = s"SurgeEmbeddedEngineTestHelper-${UUID.randomUUID()}"
  private val consumerConfig = UltiKafkaConsumerConfig(consumerGroupName, offsetReset = "latest", pollDuration = 1.second)

  private val sub = KafkaBytesConsumer(kafkaBrokers.asScala, consumerConfig, Map.empty)

  val eventBus: EventBus[EventMessage[Event]] = new EventBus[EventMessage[Event]]
  val stateStore: StateStore[AggId, StatePlusMetadata[Agg]] = new StateStore[AggId, StatePlusMetadata[Agg]]

  // Put the subscription in its own thread so it doesn't block anything else
  Future {
    sub.subscribe(ultiBusinessLogic.eventsTopic, { record ⇒
      // FIXME in apps already using the CMP libraries, it looks like the CMP serializer is automatically wired in
      //  somehow.  When we read events from those apps, they're double-wrapped in an envelope.  When we integrate
      //  with the CMP libraries, that should get fixed.
      JsonUtils.parseMaybeCompressedBytes[EventMessage[Event]](record.value()) match {
        case Some(eventMessage) ⇒
          eventBus.send(eventMessage)
        case _ ⇒
          log.error(s"Unable to interpret value from Kafka ${record.value()}")
      }
    })
  }(ExecutionContext.global)

  private val kafkaPublisher: KafkaBytesProducer = KafkaBytesProducer(kafkaBrokers.asScala, ultiBusinessLogic.eventsTopic)

  def waitForEvent[T](`type`: Class[T]): T = {
    var event: Option[T] = None
    await().atMost(timeoutSeconds, TimeUnit.SECONDS) until {
      eventBus.consumeOne().map(_.body) match {
        case Some(evt) if evt.getClass == `type` ⇒
          event = Some(evt.asInstanceOf[T])
        case _ ⇒
        // Do nothing
      }
      event.nonEmpty
    }

    event.getOrElse(throw new SurgeAssertionError(s"Event of type ${`type`} not found in the eventbus"))
  }

  def clearReceivedEvents(): Unit = {
    eventBus.clear()
  }

  // TODO make this more user friendly when using the CMP libraries
  def publishEvent(event: Event, props: EventProperties, typeInfo: EventMessageTypeInfo): Unit = {
    val key = s"${props.aggregateId.getOrElse(EmptyUUID)}:${props.sequenceNumber}"
    val value = JsonUtils.gzip(EventMessage.create(props, event, typeInfo))
    kafkaPublisher.putKeyValue(key, value)
  }

  def close(): Unit = {
    sub.stop()
  }

}
