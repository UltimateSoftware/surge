// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl

import play.api.libs.json._
import surge.core.command.SurgeCommandKafkaConfig
import surge.core._
import surge.kafka.KafkaTopic
import surge.scaladsl.command.{ AggregateCommandModel, SurgeCommandBusinessLogic }
import surge.serialization.{ Deserializer, PlayJsonDeserializer, PlayJsonSerializer, Serializer }

import scala.util.{ Failure, Success, Try }

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

  case class FailEventHandling(aggregateId: String) extends BaseTestCommand
  case class FailCommandProcessing(failProcessingId: String, withError: Throwable) extends BaseTestCommand {
    val aggregateId: String = failProcessingId
  }

  case class CreateExceptionThrowingEvent(aggregateId: String, throwable: Throwable) extends BaseTestCommand

  implicit val countIncrementedFormat: Format[CountIncremented] = Json.format
  implicit val countDecrementedFormat: Format[CountDecremented] = Json.format
  implicit val noopFormat: Format[NoOpEvent] = Json.format
  implicit val exceptionThrowingFormat: Format[ExceptionThrowingEvent] = new Format[ExceptionThrowingEvent] {
    override def writes(o: ExceptionThrowingEvent): JsValue = JsNull
    override def reads(json: JsValue): JsResult[ExceptionThrowingEvent] = JsError("Exception throwing event should never be serialized")
  }
  implicit val baseEventFormat: Format[BaseTestEvent] = Json.format
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

  trait BusinessLogicTrait extends AggregateCommandModel[State, BaseTestCommand, BaseTestEvent] {

    override def handleEvent(agg: Option[State], evt: BaseTestEvent): Option[State] = {
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

    override def processCommand(agg: Option[State], cmd: BaseTestCommand): Try[Seq[BaseTestEvent]] = {
      val newSequenceNumber = agg.map(_.version).getOrElse(0) + 1

      cmd match {
        case Increment(aggregateId)       => Success(Seq(CountIncremented(aggregateId, incrementBy = 1, sequenceNumber = newSequenceNumber)))
        case Decrement(aggregateId)       => Success(Seq(CountDecremented(aggregateId, decrementBy = 1, sequenceNumber = newSequenceNumber)))
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
  val consumerGroup: String = "count-aggregate-consumer-group-name"
  val stateTopic: KafkaTopic = KafkaTopic("testStateTopic")
  val eventTopic: KafkaTopic = KafkaTopic("testEventsTopic")
  val aggregateName = "CountAggregate"
  val kafkaConfig: SurgeCommandKafkaConfig = SurgeCommandKafkaConfig(
    stateTopic = stateTopic,
    eventsTopic = eventTopic,
    publishStateOnly = false,
    streamsApplicationId = consumerGroup,
    clientId = "",
    transactionalIdPrefix = "test-transaction-id-prefix")

  val eventFormat: SurgeEventFormatting[BaseTestEvent] = new SurgeEventFormatting[BaseTestEvent] {
    override def readEvent(bytes: Array[Byte]): BaseTestEvent = {
      eventDeserializer().deserialize(bytes)
    }

    override def writeEvent(evt: BaseTestEvent): SerializedMessage = {
      val key = s"${evt.aggregateId}:${evt.sequenceNumber}"
      val bytesPlusHeaders = eventSerializer().serialize(evt)
      SerializedMessage(key, bytesPlusHeaders.bytes, bytesPlusHeaders.headers)
    }

    override def eventSerializer(): Serializer[BaseTestEvent] = new PlayJsonSerializer[BaseTestEvent]()
    override def eventDeserializer(): Deserializer[BaseTestEvent] = new PlayJsonDeserializer[BaseTestEvent]()
  }

  val aggregateFormat: SurgeAggregateFormatting[State] = new SurgeAggregateFormatting[State] {
    override def readState(bytes: Array[Byte]): Option[State] = {
      Some(stateDeserializer().deserialize(bytes))
    }

    override def writeState(agg: State): SerializedAggregate = {
      val bytesPlusHeaders = stateSerializer().serialize(agg)
      SerializedAggregate(bytesPlusHeaders.bytes, bytesPlusHeaders.headers)
    }

    override def stateDeserializer(): Deserializer[State] = new PlayJsonDeserializer[State]()

    override def stateSerializer(): Serializer[State] = new PlayJsonSerializer[State]()
  }

  val businessLogic: SurgeCommandBusinessLogic[String, State, BaseTestCommand, BaseTestEvent] =
    new SurgeCommandBusinessLogic[String, State, BaseTestCommand, BaseTestEvent]() {
      val businessLogicTrait: BusinessLogicTrait = new BusinessLogicTrait {}

      override def aggregateName: String = "CounterAggregate"

      override def stateTopic: KafkaTopic = kafkaConfig.stateTopic

      override def eventsTopic: KafkaTopic = kafkaConfig.eventsTopic

      override def commandModel: AggregateCommandModel[State, BaseTestCommand, BaseTestEvent] = businessLogicTrait

      override def aggregateReadFormatting: SurgeAggregateReadFormatting[State] = aggregateFormat

      override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[State] = aggregateFormat

      override def eventWriteFormatting: SurgeEventWriteFormatting[BaseTestEvent] = eventFormat
    }
}
