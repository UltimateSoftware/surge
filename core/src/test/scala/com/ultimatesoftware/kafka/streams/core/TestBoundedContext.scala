// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.util.UUID

import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.monitoring.metrics.{ NoOpMetricsProvider, NoOpsMetricsPublisher }
import com.ultimatesoftware.scala.core.utils.JsonUtils
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.{ AggregateCommandModel, CommandProcessor, SimpleJsonAggregateComposer }
import play.api.libs.json._

import scala.concurrent.duration._
import scala.util.Success

trait TestBoundedContext {

  case class State(aggregateId: UUID, count: Int, version: Int, timestamp: Instant)

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

  sealed trait BaseTestEvent {
    def aggregateId: UUID
    def sequenceNumber: Int
    def eventName: String
  }

  case class CountIncremented(aggregateId: UUID, incrementBy: Int, sequenceNumber: Int, timestamp: Instant) extends BaseTestEvent {
    val eventName: String = "countIncremented"
  }

  case class CountDecremented(aggregateId: UUID, decrementBy: Int, sequenceNumber: Int, timestamp: Instant) extends BaseTestEvent {
    val eventName: String = "countDecremented"
  }

  sealed trait BaseTestCommand {
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

  case class TimestampMeta(timestamp: Instant)

  trait BusinessLogicTrait extends AggregateCommandModel[UUID, State, BaseTestCommand, BaseTestEvent, TimestampMeta, TimestampMeta] {

    override def handleEvent: (Option[State], BaseTestEvent, TimestampMeta) ⇒ Option[State] = { (agg, evt, _) ⇒
      val current = agg.getOrElse(State(evt.aggregateId, 0, 0, Instant.now))

      val newState = evt match {
        case CountIncremented(_, incrementBy, sequenceNumber, _) ⇒
          current.copy(count = current.count + incrementBy, version = sequenceNumber)
        case CountDecremented(_, decrementBy, sequenceNumber, _) ⇒
          current.copy(count = current.count - decrementBy, version = sequenceNumber)
      }
      Some(newState)
    }

    override def aggIdFromCommand: BaseTestCommand ⇒ UUID = { _.aggregateId }
    override def cmdMetaToEvtMeta: TimestampMeta ⇒ TimestampMeta = { identity }
    override def processCommand: CommandProcessor[State, BaseTestCommand, BaseTestEvent, TimestampMeta] = { (agg, cmd, meta) ⇒
      val newSequenceNumber = agg.map(_.version).getOrElse(0) + 1
      val evt = cmd match {
        case Increment(aggregateId) ⇒ CountIncremented(aggregateId, incrementBy = 1,
          sequenceNumber = newSequenceNumber, timestamp = meta.timestamp)
        case Decrement(aggregateId) ⇒ CountDecremented(aggregateId, decrementBy = 1,
          sequenceNumber = newSequenceNumber, timestamp = meta.timestamp)
      }
      Success(Seq(evt))
    }

    val commandValidator: AsyncCommandValidator[BaseTestCommand, State] = AsyncCommandValidator[BaseTestCommand, State] { _ ⇒
      Seq.empty
    }

    val aggregateComposer: SimpleJsonAggregateComposer[UUID, State] = new SimpleJsonAggregateComposer[UUID, State](stateFormat)

    val stateKeyExtractor: JsValue ⇒ String = { json ⇒ aggregateComposer.compose(Set(json)).map(_.aggregateId.toString).getOrElse("") }
  }

  object BusinessLogic extends BusinessLogicTrait

  private val kafkaConfig = KafkaStreamsCommandKafkaConfig[BaseTestEvent](
    stateTopic = KafkaTopic("testStateTopic", compacted = false, None),
    eventsTopic = KafkaTopic("testEventsTopic", compacted = false, None),
    internalMetadataTopic = KafkaTopic("metadataTopic", compacted = false, None),
    eventKeyExtractor = { evt ⇒ s"${evt.aggregateId}:${evt.sequenceNumber}" },
    stateKeyExtractor = BusinessLogic.stateKeyExtractor)
  val formats: SurgeFormatting[BaseTestEvent] = new SurgeFormatting[BaseTestEvent] {
    override def writeEvent(evt: BaseTestEvent): Array[Byte] = JsonUtils.gzip(evt)(baseEventFormat)
  }
  val kafkaStreamsLogic = KafkaStreamsCommandBusinessLogic(
    aggregateName = "CountAggregate",
    kafka = kafkaConfig,
    model = BusinessLogic,
    formatting = formats,
    commandValidator = BusinessLogic.commandValidator,
    aggregateValidator = { (_, _, _) ⇒ true },
    aggregateComposer = BusinessLogic.aggregateComposer,
    metricsProvider = NoOpMetricsProvider,
    metricsPublisher = NoOpsMetricsPublisher, metricsInterval = 100.seconds)

}
