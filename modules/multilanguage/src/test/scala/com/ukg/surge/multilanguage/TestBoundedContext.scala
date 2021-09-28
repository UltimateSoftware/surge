// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import com.google.protobuf.ByteString
import com.ukg.surge.multilanguage.protobuf._
import play.api.libs.json._

import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

object TestBoundedContext {

  case class AggregateState(aggregateId: String, count: Int, version: Int)
  implicit val stateFormat: Format[AggregateState] = Json.format

  sealed trait BaseTestCommand {
    def aggregateId: String
    def expectedVersion: Int = 0
  }

  object BaseTestCommand {
    implicit val incrementFormat: Format[Increment] = Json.format
    implicit val decrementFormat: Format[Decrement] = Json.format
    implicit val exceptionThrowingCommandFormat: Format[ExceptionThrowingCommand] = Json.format
    implicit val format: Format[BaseTestCommand] = Format[BaseTestCommand](
      Reads { js =>
        val commandType = (JsPath \ "commandType").read[String].reads(js)
        commandType.fold(
          errors => JsError("undefined commandType"),
          {
            case "increment" => (JsPath \ "data").read[Increment].reads(js)
            case "decrement" => (JsPath \ "data").read[Decrement].reads(js)
          })
      },
      Writes {
        case i: Increment => JsObject(Seq("commandType" -> JsString("increment"), "data" -> incrementFormat.writes(i)))
        case d: Decrement => JsObject(Seq("commandType" -> JsString("decrement"), "data" -> decrementFormat.writes(d)))
        case e: ExceptionThrowingCommand =>
          JsObject(Seq("commandType" -> JsString("exceptionThrowingCommand"), "data" -> exceptionThrowingCommandFormat.writes(e)))
      })
  }

  case class Increment(aggregateId: String) extends BaseTestCommand

  case class Decrement(aggregateId: String) extends BaseTestCommand

  case class ExceptionThrowingCommand(aggregateId: String) extends BaseTestCommand

  implicit val countIncrementedFormat: Format[CountIncremented] = Json.format
  implicit val countDecrementedFormat: Format[CountDecremented] = Json.format

  implicit val baseEventFormat: Format[BaseTestEvent] = Json.format
  sealed trait BaseTestEvent {
    def aggregateId: String
    def sequenceNumber: Int
    def eventName: String
  }

  object BaseTestEvent {
    implicit val format: OFormat[BaseTestEvent] = Json.format[BaseTestEvent]
  }

  case class CountIncremented(aggregateId: String, incrementBy: Int, sequenceNumber: Int) extends BaseTestEvent {
    val eventName: String = "countIncremented"
  }

  case class CountDecremented(aggregateId: String, decrementBy: Int, sequenceNumber: Int) extends BaseTestEvent {
    val eventName: String = "countDecremented"
  }
}

trait TestBoundedContext {
  import TestBoundedContext._

  class TestBusinessLogicService(implicit system: ActorSystem) extends BusinessLogicService {

    import system.dispatcher

    private def applyEvent(state: Option[AggregateState], evt: BaseTestEvent): Option[AggregateState] = {
      val currentState = state.getOrElse(AggregateState(evt.aggregateId, 0, 0))
      val newState = evt match {
        case CountIncremented(_, incrementBy, sequenceNumber) => currentState.copy(count = currentState.count + incrementBy, version = sequenceNumber)
        case CountDecremented(_, decrementBy, sequenceNumber) => currentState.copy(count = currentState.count - decrementBy, version = sequenceNumber)
      }
      Some(newState)
    }

    private def applyEvents(state: Option[AggregateState], events: Seq[BaseTestEvent]): Option[AggregateState] = {
      events.foldLeft(state)((s, e) => applyEvent(s, e))
    }

    private def applyCommand(state: Option[AggregateState], cmd: BaseTestCommand) = {
      val newSequenceNumber = state.map(_.version).getOrElse(0) + 1
      val events: Either[String, Seq[BaseTestEvent]] = Right(cmd match {
        case Increment(aggregateId) => Seq(CountIncremented(aggregateId, incrementBy = 1, sequenceNumber = newSequenceNumber))
        case Decrement(aggregateId) => Seq(CountDecremented(aggregateId, decrementBy = 1, sequenceNumber = newSequenceNumber))
      })
      val result = events.map(evts => (evts, applyEvents(state, evts)))
      result
    }

    private def serializeEvent(aggregateId: String, e: BaseTestEvent): Future[Event] = {
      val tryByteArray = Try(Json.toJson(e).toString().getBytes())
      val result = tryByteArray.map(byteArray => Event(aggregateId).withPayload(ByteString.copyFrom(byteArray)))
      Future.fromTry(result)
    }

    private def deserializeEvent(e: Event): Future[BaseTestEvent] = {
      val result = Try(Json.parse(e.payload.toByteArray).as[BaseTestEvent])
      Future.fromTry(result)
    }

    private def serializeState(aggregateId: String, maybeState: Option[AggregateState]): Future[Option[State]] = {
      Future.successful(maybeState.map { state =>
        val serializedState = Json.toJson(state).toString().getBytes()
        State(aggregateId, payload = ByteString.copyFrom(serializedState))
      })
    }

    private def toProcessCommandReply(
        aggregateId: String,
        result: Either[String, (Seq[BaseTestEvent], Option[AggregateState])]): Future[ProcessCommandReply] = {
      result match {
        case Left(rejMsg: String) =>
          Future.successful(ProcessCommandReply(aggregateId, isSuccess = false, rejMsg))
        case Right(result: (Seq[BaseTestEvent], Option[AggregateState])) =>
          for {
            events <- Future.sequence(result._1.map(serializeEvent(aggregateId, _)))
            state <- serializeState(aggregateId, result._2)
          } yield ProcessCommandReply(aggregateId, isSuccess = true, events = events, newState = state)
      }
    }

    override def processCommand(in: ProcessCommandRequest): Future[ProcessCommandReply] = {
      (in.state, in.command) match {
        case (maybePbState, Some(pbCommand)) =>
          val aggregateId: String = pbCommand.aggregateId
          val stateOpt = maybePbState.flatMap { pbState =>
            Json.parse(pbState.payload.toByteArray).asOpt[AggregateState]
          }
          val commandT = Try(Json.parse(pbCommand.payload.toByteArray).as[BaseTestCommand])
          (for {
            cmd <- commandT
            result <- Try(applyCommand(stateOpt, cmd))
          } yield result) match {
            case Failure(exception) => Future.failed(exception)
            case Success(result) =>
              toProcessCommandReply(aggregateId, result)
          }
        case (_, None) =>
          Future.failed(new Exception("No command provided in ProcessCommandRequest"))
      }
    }

    override def handleEvents(in: HandleEventsRequest): Future[HandleEventsResponse] = {
      val stateOpt = in.state.flatMap { pbState =>
        Json.parse(pbState.payload.toByteArray).asOpt[AggregateState]
      }
      for {
        events <- Future.sequence(in.events.map(deserializeEvent))
        newState <- Future.fromTry(Try(applyEvents(stateOpt, events)))
        newStateProtobuf <- serializeState(in.aggregateId, newState)
        response = HandleEventsResponse(aggregateId = in.aggregateId, state = newStateProtobuf)
      } yield response
    }
  }
}
