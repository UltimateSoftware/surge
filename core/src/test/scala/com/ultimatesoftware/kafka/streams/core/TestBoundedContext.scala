/*
 * Copyright (C) 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>
 */

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.util.UUID

import com.ultimatesoftware.scala.core.domain._
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.messaging.{ BaseCommand ⇒ _, BaseEvent ⇒ _, _ }
import com.ultimatesoftware.scala.core.monitoring.metrics.NoOpsMetricsPublisher
import com.ultimatesoftware.scala.core.resource.BaseResource
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import play.api.libs.json._

import scala.concurrent.duration._
import scala.util.{ Success, Try }

trait TestBoundedContext {

  case class State(aggregateId: UUID, count: Int, version: Int, timestamp: Instant) extends BaseAggregate {
    override def isDeleted: Boolean = false
    override def id: UUID = aggregateId
  }

  implicit val stateFormat: Format[State] = Json.format
  implicit val countIncrementedFormat: Format[CountIncremented] = Json.format
  implicit val countDecrementedFormat: Format[CountDecremented] = Json.format
  implicit val incrementFormat: Format[Increment] = Json.format
  implicit val decrementFormat: Format[Decrement] = Json.format

  val baseEventFormat: Format[BaseTestEvent] = new Format[BaseTestEvent] {
    override def reads(json: JsValue): JsResult[BaseTestEvent] = {
      Json.fromJson[CountIncremented](json) orElse
        Json.fromJson[CountDecremented](json)
    }

    override def writes(o: BaseTestEvent): JsValue = {
      o match {
        case inc: CountIncremented ⇒ Json.toJson(inc)
        case dec: CountDecremented ⇒ Json.toJson(dec)
      }
    }
  }

  val baseCommandFormat: Format[BaseTestCommand] = new Format[BaseTestCommand] {
    override def writes(o: BaseTestCommand): JsValue = {
      o match {
        case inc: Increment ⇒ Json.toJson(inc)
        case dec: Decrement ⇒ Json.toJson(dec)
      }
    }

    override def reads(json: JsValue): JsResult[BaseTestCommand] = {
      Json.fromJson[Increment](json) orElse
        Json.fromJson[Decrement](json)
    }
  }

  sealed trait BaseTestEvent extends BaseEvent[State] {
    def aggregateId: UUID
    def eventName: String

    override def typeInfo: EventMessageTypeInfo = {
      DetailedEventMessageTypeInfo(boundedContext = "test", aggregateName = "counter", eventName = eventName, schemaVersion = "1.0.0")
    }
  }

  case class CountIncremented(aggregateId: UUID, incrementBy: Int, version: Int, timestamp: Instant) extends BaseTestEvent {
    val eventName: String = "countIncremented"
  }

  case class CountDecremented(aggregateId: UUID, decrementBy: Int, version: Int, timestamp: Instant) extends BaseTestEvent {
    val eventName: String = "countDecremented"
  }

  sealed trait BaseTestCommand extends BaseCommand[State] {
    def aggregateId: UUID
    def expectedVersion: Int
  }

  case class Increment(incrementAggregateId: UUID) extends BaseTestCommand {
    val aggregateId: UUID = incrementAggregateId
    val expectedVersion: Int = 0 // Not used
  }

  case class Decrement(decrementAggregateId: UUID) extends BaseTestCommand {
    val aggregateId: UUID = decrementAggregateId
    val expectedVersion: Int = 0 // Not used
  }

  case class EmptyResource(id: UUID) extends BaseResource[State] {
    override def version: Int = 0
  }

  trait BusinessLogicTrait extends DomainBusinessLogicAdapter[State, UUID, BaseTestCommand, BaseTestEvent, EmptyResource, DefaultCommandMetadata] {
    override def aggregateName: String = "TestCounterAggregate"

    override def processCommand(
      currentAggregate: Option[State],
      command: BaseTestCommand, props: DefaultCommandMetadata): Try[Seq[BaseTestEvent]] = {
      val newSequenceNumber = currentAggregate.map(_.version).getOrElse(0) + 1
      val evt = command match {
        case Increment(aggregateId) ⇒ CountIncremented(aggregateId, incrementBy = 1,
          version = newSequenceNumber, timestamp = props.timestamp)
        case Decrement(aggregateId) ⇒ CountDecremented(aggregateId, decrementBy = 1,
          version = newSequenceNumber, timestamp = props.timestamp)
      }
      Success(Seq(evt))
    }

    override def processEvent(currentAggregate: Option[State], eventMessage: EventMessage[BaseTestEvent]): Option[State] = {
      val event = eventMessage.body
      val current = currentAggregate.getOrElse(State(event.aggregateId, 0, 0, Instant.now))

      val newState = event match {
        case CountIncremented(_, incrementBy, sequenceNumber, _) ⇒
          current.copy(count = current.count + incrementBy, version = sequenceNumber)
        case CountDecremented(_, decrementBy, sequenceNumber, _) ⇒
          current.copy(count = current.count - decrementBy, version = sequenceNumber)
      }
      Some(newState)
    }

    override implicit def eventFormat: Format[BaseTestEvent] = baseEventFormat
    override implicit def commandFormat: Format[BaseTestCommand] = baseCommandFormat

    override def resourceFromAggregate(aggregate: Option[State]): Option[EmptyResource] = None
    override implicit def resourceFormat: Format[EmptyResource] = Json.format[EmptyResource]

    override val commandValidator: AsyncCommandValidator[BaseTestCommand, State] = AsyncCommandValidator[BaseTestCommand, State] { _ ⇒
      Seq.empty
    }

    override def extractAggregateId(aggregate: State): UUID = aggregate.aggregateId
    override def extractCmdAggregateId(command: BaseTestCommand): UUID = command.aggregateId

    override def eventTypeInfo(event: BaseTestEvent): EventMessageTypeInfo = event.typeInfo

    override def metaToPartialEventProps(cmdMeta: DefaultCommandMetadata): EventProperties = cmdMeta.toEventProperties

    override def aggregateComposer: AggregateComposer[State, UUID] = new SimpleJsonComposer[State, UUID](stateFormat)
  }
  object BusinessLogic extends BusinessLogicTrait

  val kafkaStreamsLogic = KafkaStreamsCommandBusinessLogic(
    stateTopic = KafkaTopic("testStateTopic", compacted = false, None),
    eventsTopic = KafkaTopic("testEventsTopic", compacted = false, None),
    internalMetadataTopic = KafkaTopic("metadataTopic", compacted = false, None),
    businessLogicAdapter = BusinessLogic,
    metricsPublisher = NoOpsMetricsPublisher, metricsInterval = 100.seconds)

}
