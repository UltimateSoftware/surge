// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.util.UUID

import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.monitoring.metrics.{ NoOpMetricsProvider, NoOpsMetricsPublisher }
import com.ultimatesoftware.scala.core.validations.{ AsyncCommandValidator, AsyncValidationResult, ValidationError }
import com.ultimatesoftware.scala.oss.domain.{ AggregateCommandModel, CommandProcessor }
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object TestBoundedContext {
  case class State(aggregateId: UUID, count: Int, version: Int, timestamp: Instant)
  implicit val stateFormat: Format[State] = Json.format

  sealed trait BaseTestCommand {
    def aggregateId: UUID
    def expectedVersion: Int = 0
    def validate: Seq[AsyncValidationResult[_]] = Seq.empty
  }

  case class Increment(incrementAggregateId: UUID) extends BaseTestCommand {
    val aggregateId: UUID = incrementAggregateId
  }

  case class Decrement(decrementAggregateId: UUID) extends BaseTestCommand {
    val aggregateId: UUID = decrementAggregateId
  }

  case class DoNothing(aggregateId: UUID) extends BaseTestCommand

  case class CauseInvalidValidation(aggregateId: UUID) extends BaseTestCommand {
    val validationErrors: Seq[ValidationError] = Seq(ValidationError("This command is invalid"))
    override def validate: Seq[AsyncValidationResult[_]] = Seq(
      Future.successful(Left(validationErrors)))
  }
  case class FailCommandProcessing(failProcessingId: UUID, withError: RuntimeException) extends BaseTestCommand {
    val aggregateId: UUID = failProcessingId
  }

  case class TimestampMeta(timestamp: Instant)
}

trait TestBoundedContext {
  import TestBoundedContext._
  implicit val countIncrementedFormat: Format[CountIncremented] = Json.format
  implicit val countDecrementedFormat: Format[CountDecremented] = Json.format

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

      cmd match {
        case Increment(aggregateId) ⇒ Success(Seq(CountIncremented(aggregateId, incrementBy = 1,
          sequenceNumber = newSequenceNumber, timestamp = meta.timestamp)))
        case Decrement(aggregateId) ⇒ Success(Seq(CountDecremented(aggregateId, decrementBy = 1,
          sequenceNumber = newSequenceNumber, timestamp = meta.timestamp)))
        case _: DoNothing ⇒ Success(Seq.empty)
        case fail: FailCommandProcessing ⇒
          Failure(fail.withError)
        case _ ⇒
          throw new RuntimeException("Received unexpected message in command handler! This should not happen and indicates a bad test")
      }
    }

    val commandValidator: AsyncCommandValidator[BaseTestCommand, State] = AsyncCommandValidator[BaseTestCommand, State] { cmd ⇒
      cmd.msg.validate
    }
  }

  object BusinessLogic extends BusinessLogicTrait

  private val kafkaConfig = KafkaStreamsCommandKafkaConfig(
    stateTopic = KafkaTopic("testStateTopic", compacted = false, None),
    eventsTopic = KafkaTopic("testEventsTopic", compacted = false, None))

  val readFormats: SurgeReadFormatting[UUID, State, BaseTestEvent, TimestampMeta] = new SurgeReadFormatting[UUID, State, BaseTestEvent, TimestampMeta] {
    override def readEvent(bytes: Array[Byte]): (BaseTestEvent, Option[TimestampMeta]) = {
      (Json.parse(bytes).as[BaseTestEvent](baseEventFormat), None)
    }

    override def readState(bytes: Array[Byte]): Option[State] = {
      Json.parse(bytes).asOpt[State]
    }
  }

  val writeFormats: SurgeWriteFormatting[UUID, State, BaseTestEvent, TimestampMeta] = new SurgeWriteFormatting[UUID, State, BaseTestEvent, TimestampMeta] {
    override def writeEvent(evt: BaseTestEvent, metadata: TimestampMeta): SerializedMessage = {
      val key = s"${evt.aggregateId}:${evt.sequenceNumber}"
      val body = Json.toJson(evt)(baseEventFormat).toString().getBytes()
      SerializedMessage(key, body, Map.empty)
    }

    override def writeState(agg: State): Array[Byte] = Json.toJson(agg).toString().getBytes()
  }
  val kafkaStreamsLogic: KafkaStreamsCommandBusinessLogic[UUID, State, BaseTestCommand, BaseTestEvent, TimestampMeta, TimestampMeta] =
    KafkaStreamsCommandBusinessLogic(
      aggregateName = "CountAggregate",
      kafka = kafkaConfig,
      model = BusinessLogic,
      readFormatting = readFormats,
      writeFormatting = writeFormats,
      commandValidator = BusinessLogic.commandValidator,
      aggregateValidator = { (_, _, _) ⇒ true },
      metricsProvider = NoOpMetricsProvider,
      metricsPublisher = NoOpsMetricsPublisher,
      metricsInterval = 100.seconds,
      consumerGroup = "count-aggregate-consumer-group-name",
      transactionalIdPrefix = "test-transaction-id-prefix")
}
