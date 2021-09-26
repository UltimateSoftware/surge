package com.ukg.surge.multilanguage

import com.google.protobuf.ByteString
import com.ukg.surge.multilanguage.protobuf._
import play.api.libs.json._
import surge.core._
import surge.core.command.SurgeCommandKafkaConfig
import surge.kafka.KafkaTopic
import surge.scaladsl.command.{AggregateCommandModel, SurgeCommandBusinessLogic}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

object TestBoundedContext {

  case class AggregateState(aggregateId: String, count: Int, version: Int)
  implicit val stateFormat: Format[AggregateState] = Json.format

  sealed trait BaseTestCommand {
    def aggregateId: String
    def expectedVersion: Int = 0
  }

  case class Increment(incrementAggregateId: String) extends BaseTestCommand {
    val aggregateId: String = incrementAggregateId
  }
  implicit val incrementFormat: Format[Increment] = Json.format

  case class CreateExceptionThrowingEvent(aggregateId: String, throwable: Throwable) extends BaseTestCommand

  implicit val countIncrementedFormat: Format[CountIncremented] = Json.format
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

  case class ExceptionThrowingEvent(aggregateId: String, sequenceNumber: Int, throwable: Throwable) extends BaseTestEvent {
    val eventName: String = "exception-throwing"
  }
}

trait TestBoundedContext {
  import TestBoundedContext._
  implicit def ec: ExecutionContext

  trait BusinessLogicTrait extends BusinessLogicService {

    private def applyEvent(state: Option[AggregateState], evt: CountIncremented): Option[AggregateState] = {
      state.map(st => st.copy(count = st.count + evt.incrementBy, version = evt.sequenceNumber))
    }

    private def applyCommand(state: Option[AggregateState], cmd: Increment) = {
      val newSequenceNumber = state.map(_.version).getOrElse(0) + 1
      val commandHandlerResult: Either[String, Seq[CountIncremented]] = Right(Seq(CountIncremented(cmd.aggregateId, incrementBy = 1, sequenceNumber = newSequenceNumber)))
      val result = commandHandlerResult.map(events => (events, events.foldLeft(state)((state, evt) => applyEvent(state, evt))))
      result
    }

    private def convertEvent(aggregateId: String, e: CountIncremented): Future[Event] = {
      val tryByteArray = Try(Json.toJson(e).toString().getBytes())
      val result = tryByteArray.map(byteArray => Event(aggregateId).withPayload(ByteString.copyFrom(byteArray)))
      Future.fromTry(result)
    }

    private def convertState(aggregateId: String, maybeState: Option[AggregateState]): Future[Option[State]] = {
      maybeState match {
        case Some(state) =>
          Future
            .fromTry(Try(Json.toJson(state).toString().getBytes()))
            .map((byteArray: Array[Byte]) =>
              Some {
                State(aggregateId, payload = ByteString.copyFrom(byteArray))
              })
        case None =>
          Future.successful(Option.empty[State])
      }
    }

    private def toProcessCommandReply(aggregateId: String, result: Either[String, (Seq[CountIncremented], Option[AggregateState])]): Future[ProcessCommandReply] = {
      result match {
        case Left(rejMsg: String) =>
          Future.successful(ProcessCommandReply(aggregateId, isSuccess = false, rejMsg))
        case Right(result: (Seq[CountIncremented], Option[AggregateState])) =>
          for {
            events <- Future.sequence(result._1.map(convertEvent(aggregateId, _)))
            state <- convertState(aggregateId, result._2)
          } yield ProcessCommandReply(aggregateId, isSuccess = true, events = events, newState = state)
      }
    }

    override def processCommand(in: ProcessCommandRequest): Future[ProcessCommandReply] = {
      // mock will receive only one type of command
      (in.state, in.command) match {
        case (maybePbState, Some(pbCommand)) =>
          val aggregateId: String = pbCommand.aggregateId
          val tryState: Try[Option[AggregateState]] = maybePbState match {
            case Some(pbState) =>
              Try(Json.parse(pbState.payload.toByteArray).asOpt[AggregateState])
            case None =>
              Success(Option.empty[AggregateState])
          }
          val tryCommand = Try(Json.parse(pbCommand.payload.toByteArray).as[Increment])
            (for {
              state <- tryState
              cmd <- tryCommand
              result <- Try(applyCommand(state, cmd))
            } yield result) match {
              case Failure(exception) => Future.failed(exception)
              case Success(result) =>
                toProcessCommandReply(aggregateId, result)
            }
        case (_, None) =>
          Future.failed(new Exception("No command provided in ProcessCommandRequest"))
      }
    }

    override def handleEvents(in: HandleEventsRequest): Future[HandleEventsResponse] = ???
  }

  object BusinessLogic extends BusinessLogicTrait
//
//  val consumerGroup: String = "count-aggregate-consumer-group-name"
//  val stateTopic: KafkaTopic = KafkaTopic("testStateTopic")
//  val eventTopic: KafkaTopic = KafkaTopic("testEventsTopic")
//  val aggregateName = "CountAggregate"
//  val kafkaConfig = SurgeCommandKafkaConfig(
//    stateTopic = stateTopic,
//    eventsTopic = eventTopic,
//    publishStateOnly = false,
//    streamsApplicationId = consumerGroup,
//    clientId = "",
//    transactionalIdPrefix = "test-transaction-id-prefix")
//
//  val eventFormat: SurgeEventFormatting[BaseTestEvent] = new SurgeEventFormatting[BaseTestEvent] {
//    override def readEvent(bytes: Array[Byte]): BaseTestEvent = {
//      Json.parse(bytes).as[BaseTestEvent]
//    }
//
//    override def writeEvent(evt: BaseTestEvent): SerializedMessage = {
//      val key = s"${evt.aggregateId}:${evt.sequenceNumber}"
//      val body = Json.toJson(evt).toString().getBytes()
//      SerializedMessage(key, body, Map.empty)
//    }
//  }
//
//  val aggregateFormat: SurgeAggregateFormatting[AggregateState] = new SurgeAggregateFormatting[AggregateState] {
//    override def readState(bytes: Array[Byte]): Option[AggregateState] = {
//      Json.parse(bytes).asOpt[AggregateState]
//    }
//
//    override def writeState(agg: AggregateState): SerializedAggregate = SerializedAggregate(Json.toJson(agg).toString().getBytes(), Map.empty)
//  }
//  val businessLogic: SurgeCommandBusinessLogic[String, AggregateState, BaseTestCommand, BaseTestEvent] =
//    new SurgeCommandBusinessLogic[String, AggregateState, BaseTestCommand, BaseTestEvent]() {
//      val businessLogicTrait: BusinessLogicTrait = new BusinessLogicTrait {}
//
//      override def aggregateName: String = "CounterAggregate"
//
//      override def stateTopic: KafkaTopic = kafkaConfig.stateTopic
//
//      override def eventsTopic: KafkaTopic = kafkaConfig.eventsTopic
//
//      override def commandModel: AggregateCommandModel[AggregateState, BaseTestCommand, BaseTestEvent] = businessLogicTrait
//
//      override def aggregateReadFormatting: SurgeAggregateReadFormatting[AggregateState] = aggregateFormat
//
//      override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[AggregateState] = aggregateFormat
//
//      override def eventWriteFormatting: SurgeEventWriteFormatting[BaseTestEvent] = eventFormat
//    }
}
