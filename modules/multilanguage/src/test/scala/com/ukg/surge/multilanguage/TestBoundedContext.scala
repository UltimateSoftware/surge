// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import com.google.protobuf.ByteString
import com.ukg.surge.multilanguage.protobuf._
import play.api.libs.json._
import surge.core._
import surge.core.command.SurgeCommandKafkaConfig
import surge.kafka.KafkaTopic
import surge.scaladsl.command.{ AggregateCommandModel, SurgeCommandBusinessLogic }

import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future }
import scala.util.{ Failure, Success, Try }

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

  class TestBusinessLogicService(implicit system: ActorSystem) extends BusinessLogicService {

    import system.dispatcher

    private def applyEvent(state: Option[AggregateState], evt: CountIncremented): Option[AggregateState] = {
      state match {
        case Some(agg) => Some(agg.copy(count = agg.count + evt.incrementBy, version = evt.sequenceNumber))
        case None => Some(AggregateState(evt.aggregateId, evt.incrementBy, evt.sequenceNumber))
      }
    }

    private def applyEvents(state: Option[AggregateState], events: Seq[CountIncremented]): Option[AggregateState] = {
      events.foldLeft(state)((s, e) => applyEvent(s, e))
    }

    private def applyCommand(state: Option[AggregateState], cmd: Increment) = {
      val newSequenceNumber = state.map(_.version).getOrElse(0) + 1
      val events: Either[String, Seq[CountIncremented]] = Right(
        Seq(CountIncremented(cmd.aggregateId, incrementBy = 1, sequenceNumber = newSequenceNumber)))
      val result = events.map(evts => (evts, applyEvents(state, evts)))
      result
    }

    private def serializeEvent(aggregateId: String, e: CountIncremented): Future[Event] = {
      val tryByteArray = Try(Json.toJson(e).toString().getBytes())
      val result = tryByteArray.map(byteArray => Event(aggregateId).withPayload(ByteString.copyFrom(byteArray)))
      Future.fromTry(result)
    }

    private def deserializeEvent(e: Event): Future[CountIncremented] = {
      val result = Try(Json.parse(e.payload.toByteArray).as[CountIncremented])
      Future.fromTry(result)
    }

    private def serializeState(aggregateId: String, maybeState: Option[AggregateState]): Future[Option[State]] = {
      Future.successful(
        maybeState.map { state =>
          val serializedState = Json.toJson(state).toString().getBytes()
          State(aggregateId, payload = ByteString.copyFrom(serializedState))
        }
      )
    }

    private def toProcessCommandReply(
        aggregateId: String,
        result: Either[String, (Seq[CountIncremented], Option[AggregateState])]): Future[ProcessCommandReply] = {
      result match {
        case Left(rejMsg: String) =>
          Future.successful(ProcessCommandReply(aggregateId, isSuccess = false, rejMsg))
        case Right(result: (Seq[CountIncremented], Option[AggregateState])) =>
          for {
            events <- Future.sequence(result._1.map(serializeEvent(aggregateId, _)))
            state <- serializeState(aggregateId, result._2)
          } yield ProcessCommandReply(aggregateId, isSuccess = true, events = events, newState = state)
      }
    }

    override def processCommand(in: ProcessCommandRequest): Future[ProcessCommandReply] = {
      // mock will receive only one type of command
      (in.state, in.command) match {
        case (maybePbState, Some(pbCommand)) =>
          val aggregateId: String = pbCommand.aggregateId
          val stateOpt = maybePbState.flatMap {pbState =>
            Json.parse(pbState.payload.toByteArray).asOpt[AggregateState]
          }
          val commandT = Try(Json.parse(pbCommand.payload.toByteArray).as[Increment])
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
      val stateOpt = in.state.flatMap {pbState =>
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
