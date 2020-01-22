// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl.test

import java.util.UUID
import java.util.concurrent.{ Callable, CompletionStage, TimeUnit }

import com.ultimatesoftware.kafka.streams.javadsl.KafkaStreamsCommand
import com.ultimatesoftware.mp.domain.{ MetadataInfoFromUltiEvent, TypeInfoFromUltiEventAnnotation }
import com.ultimatesoftware.mp.messaging.MessageCreator
import com.ultimatesoftware.mp.messaging.format.{ MessagingPlatformEventMessageWriteFormatting, MessagingPlatformEventReadFormatting }
import com.ultimatesoftware.mp.props.MessagingPlatformProperties
import com.ultimatesoftware.mp.serialization.`type`.TypeResolver
import com.ultimatesoftware.scala.core.domain.DefaultCommandMetadata
import com.ultimatesoftware.scala.core.kafka._
import com.ultimatesoftware.scala.core.messaging.{ EventMessageTypeInfo, EventProperties }
import com.ultimatesoftware.scala.core.utils.EmptyUUID
import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.Awaitility._
import org.awaitility.core.ThrowingRunnable
import org.slf4j.{ Logger, LoggerFactory }

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.{ Success, Try }

class IntegrationTestHelper[Event](
    kafkaBrokers: java.util.List[String],
    typeResolver: TypeResolver,
    eventsTopic: KafkaTopic) {
  private val timeoutSeconds = 30
  private val log: Logger = LoggerFactory.getLogger(getClass)

  private implicit def byNameFunctionToCallableOfType[T](function: ⇒ T): Callable[T] = () ⇒ function
  private implicit def byNameFunctionToCallableOfBoolean(function: ⇒ scala.Boolean): Callable[java.lang.Boolean] = () ⇒ function
  private implicit def byNameFunctionToRunnable[T](function: ⇒ T): ThrowingRunnable = () ⇒ function

  private val eventWrite = MessagingPlatformEventMessageWriteFormatting.create
  private val eventRead = MessagingPlatformEventReadFormatting.create[Event](typeResolver)

  private val consumerGroupName = s"SurgeEmbeddedEngineTestHelper-${UUID.randomUUID()}"
  private val consumerConfig = UltiKafkaConsumerConfig(consumerGroupName, offsetReset = "latest", pollDuration = 1.second)

  private val sub = KafkaBytesConsumer(kafkaBrokers.asScala, consumerConfig, Map.empty)

  val eventBus: EventBus[Event] = new EventBus[Event]

  // Put the subscription in its own thread so it doesn't block anything else
  Future {
    sub.subscribe(eventsTopic, { record ⇒
      Try(eventRead.readEvent(record.value())._1) match {
        case Success(message) ⇒
          eventBus.send(message)
        case _ ⇒
          log.error(s"Unable to interpret value from Kafka ${record.value()}")
      }
    })
  }(ExecutionContext.global)

  private val kafkaPublisher: KafkaBytesProducer = KafkaBytesProducer(kafkaBrokers.asScala, eventsTopic)

  def waitForEvent[T](`type`: Class[T]): T = {
    var event: Option[T] = None
    await().atMost(timeoutSeconds, TimeUnit.SECONDS) until {
      eventBus.consumeOne() match {
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

  private val cmpMessageCreator = new MessageCreator[Event](MessagingPlatformProperties.create)
  def publishEvent(event: Event): CompletionStage[KafkaRecordMetadata[String]] = {
    publishEvent(event, eventsTopic)
  }

  def publishEvent(event: Event, topic: KafkaTopic): CompletionStage[KafkaRecordMetadata[String]] = {
    val props = DefaultCommandMetadata.empty().toEventProperties
    val typeInfo = TypeInfoFromUltiEventAnnotation.extract(event)
    publishEvent(event, props, typeInfo, topic)
  }

  def publishEvent(event: Event, props: EventProperties, typeInfo: EventMessageTypeInfo): CompletionStage[KafkaRecordMetadata[String]] = {
    publishEvent(event, props, typeInfo, eventsTopic)
  }
  def publishEvent(event: Event, props: EventProperties,
    typeInfo: EventMessageTypeInfo, topic: KafkaTopic): CompletionStage[KafkaRecordMetadata[String]] = {
    val key = s"${props.aggregateId.getOrElse(EmptyUUID)}:${props.sequenceNumber}"
    val metaInfo = MetadataInfoFromUltiEvent.extract(event)
    val message = cmpMessageCreator.createMessage(event, props, typeInfo, metaInfo, None)

    val value = eventWrite.writeEvent(message, props)

    publishRecord(new ProducerRecord[String, Array[Byte]](topic.name, key, value))
  }
  private def publishRecord(record: ProducerRecord[String, Array[Byte]]): CompletionStage[KafkaRecordMetadata[String]] = {
    kafkaPublisher.putRecord(record).toJava
  }

  def close(): Unit = {
    sub.stop()
  }
}

class CommandIntegrationTestHelper[AggId, Event](kafkaBrokers: java.util.List[String], typeResolver: TypeResolver,
    eventsTopic: KafkaTopic, commandEngine: KafkaStreamsCommand[AggId, _, _, _, _, _])
  extends IntegrationTestHelper[Event](kafkaBrokers, typeResolver, eventsTopic) {

  def awaitAggregateCreation(aggId: AggId): Unit = {
    awaitAggregateCreation(aggId, 5000)
  }

  def awaitAggregateCreation(aggId: AggId, atMostMillis: Int): Unit = {
    await().atMost(atMostMillis, TimeUnit.MILLISECONDS) until {
      new Callable[java.lang.Boolean] {
        override def call(): java.lang.Boolean = {
          implicit val ec: ExecutionContext = ExecutionContext.global
          val aggState = Await.result(commandEngine.aggregateFor(aggId).getState.toScala, atMostMillis.millis)
          aggState.isPresent
        }
      }
    }
  }
}
