// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import io.opentracing.mock.MockTracer
import play.api.libs.json._
import surge.internal.domain.CommandProcessor
import surge.internal.domain.AggregateCommandModel
import surge.internal.utils.JsonFormats
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

import scala.util.{ Failure, Success }

object TestBoundedContext {
  case class State(aggregateId: String, count: Int, version: Int)
  implicit val stateFormat: Format[State] = Json.format

  sealed trait BaseTestCommand {
    def aggregateId: String
    def expectedVersion: Int = 0
  }

  case class Increment(incrementAggregateId: String) extends BaseTestCommand {
    val aggregateId: String = incrementAggregateId
  }

  case class Decrement(decrementAggregateId: String) extends BaseTestCommand {
    val aggregateId: String = decrementAggregateId
  }

  case class DoNothing(aggregateId: String) extends BaseTestCommand
  case class CreateNoOpEvent(aggregateId: String) extends BaseTestCommand

  case class FailCommandProcessing(failProcessingId: String, withError: Throwable) extends BaseTestCommand {
    val aggregateId: String = failProcessingId
  }
  case class CreateExceptionThrowingEvent(aggregateId: String, throwable: Throwable) extends BaseTestCommand

  sealed trait BaseTestEvent {
    def aggregateId: String
    def sequenceNumber: Int
    def eventName: String
  }

  case class CountIncremented(aggregateId: String, incrementBy: Int, sequenceNumber: Int) extends BaseTestEvent {
    val eventName: String = "countIncremented"
  }

  case class CountDecremented(aggregateId: String, decrementBy: Int, sequenceNumber: Int) extends BaseTestEvent {
    val eventName: String = "countDecremented"
  }

  case class NoOpEvent(aggregateId: String, sequenceNumber: Int) extends BaseTestEvent {
    val eventName: String = "no-op"
  }

  case class ExceptionThrowingEvent(aggregateId: String, sequenceNumber: Int, throwable: Throwable) extends BaseTestEvent {
    val eventName: String = "exception-throwing"
  }
}

trait TestBoundedContext {
  import TestBoundedContext._
  implicit val countIncrementedFormat: Format[CountIncremented] = Json.format
  implicit val countDecrementedFormat: Format[CountDecremented] = Json.format

  trait BusinessLogicTrait extends AggregateCommandModel[State, BaseTestCommand, BaseTestEvent] {

    override def handleEvent: (Option[State], BaseTestEvent) => Option[State] = { (agg, evt) =>
      val current = agg.getOrElse(State(evt.aggregateId, 0, 0))

      val newState = evt match {
        case CountIncremented(_, incrementBy, sequenceNumber) =>
          current.copy(count = current.count + incrementBy, version = sequenceNumber)
        case CountDecremented(_, decrementBy, sequenceNumber) =>
          current.copy(count = current.count - decrementBy, version = sequenceNumber)
        case _: NoOpEvent                    => current
        case ExceptionThrowingEvent(_, _, e) => throw e
      }
      Some(newState)
    }

    override def processCommand: CommandProcessor[State, BaseTestCommand, BaseTestEvent] = { (agg, cmd) =>
      val newSequenceNumber = agg.map(_.version).getOrElse(0) + 1

      cmd match {
        case Increment(aggregateId) => Success(Seq(CountIncremented(aggregateId, incrementBy = 1,
          sequenceNumber = newSequenceNumber)))
        case Decrement(aggregateId) => Success(Seq(CountDecremented(aggregateId, decrementBy = 1,
          sequenceNumber = newSequenceNumber)))
        case CreateNoOpEvent(aggregateId) => Success(Seq(NoOpEvent(aggregateId, newSequenceNumber)))
        case _: DoNothing                 => Success(Seq.empty)
        case fail: FailCommandProcessing =>
          Failure(fail.withError)
        case createExceptionEvent: CreateExceptionThrowingEvent =>
          Success(Seq(ExceptionThrowingEvent(createExceptionEvent.aggregateId, newSequenceNumber, createExceptionEvent.throwable)))
        case _ =>
          throw new RuntimeException("Received unexpected message in command handler! This should not happen and indicates a bad test")
      }
    }
  }

  object BusinessLogic extends BusinessLogicTrait

  private val kafkaConfig = SurgeCommandKafkaConfig(
    stateTopic = KafkaTopic("testStateTopic"),
    eventsTopic = KafkaTopic("testEventsTopic"),
    publishStateOnly = false,
    consumerGroup = "count-aggregate-consumer-group-name",
    clientId = "",
    transactionalIdPrefix = "test-transaction-id-prefix")

  val readFormats: SurgeReadFormatting[State, BaseTestEvent] = new SurgeReadFormatting[State, BaseTestEvent] {
    override def readEvent(bytes: Array[Byte]): BaseTestEvent = {
      Json.parse(bytes).as[BaseTestEvent](JsonFormats.jsonFormatterFromJackson)
    }

    override def readState(bytes: Array[Byte]): Option[State] = {
      Json.parse(bytes).asOpt[State]
    }
  }

  val writeFormats: SurgeWriteFormatting[State, BaseTestEvent] = new SurgeWriteFormatting[State, BaseTestEvent] {
    override def writeEvent(evt: BaseTestEvent): SerializedMessage = {
      val key = s"${evt.aggregateId}:${evt.sequenceNumber}"
      val body = Json.toJson(evt)(JsonFormats.jsonFormatterFromJackson).toString().getBytes()
      SerializedMessage(key, body, Map.empty)
    }

    override def writeState(agg: State): SerializedAggregate = SerializedAggregate(Json.toJson(agg).toString().getBytes(), Map.empty)
  }
  val businessLogic: SurgeCommandBusinessLogic[State, BaseTestCommand, BaseTestEvent] =
    SurgeCommandBusinessLogic(
      aggregateName = "CountAggregate",
      kafka = kafkaConfig,
      model = BusinessLogic,
      readFormatting = readFormats,
      writeFormatting = writeFormats,
      aggregateValidator = { (_, _, _) => true },
      metrics = Metrics.globalMetricRegistry,
      tracer = new MockTracer())
}
