// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package com.ukg.surge.multilanguage.scalasdk

import akka.actor.ActorSystem
import akka.event.Logging
import com.google.protobuf.ByteString
import com.ukg.surge.multilanguage.protobuf._

import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class BusinessServiceImpl[S, E, C](cqrsModel: CQRSModel[S, E, C], serDeser: SerDeser[S, E, C])(implicit system: ActorSystem) extends BusinessLogicService {

  private val logger = Logging(system, "BusinessServiceImpl")

  import system.dispatcher

  private[scalasdk] def convertEvent(aggregateId: String, e: E): Future[Event] = {
    val result = serDeser.serializeEvent(e).map((byteArray: Array[Byte]) => Event(aggregateId).withPayload(ByteString.copyFrom(byteArray)))
    Future.fromTry(result)
  }

  private[scalasdk] def convertEvent(e: Event): Future[E] = {
    val result = serDeser.deserializeEvent(e.payload)
    Future.fromTry(result)
  }

  private[scalasdk] def convertEvents(aggregateId: String, domainEvents: Seq[E]): Future[Seq[Event]] = {
    Future.sequence(domainEvents.map(convertEvent(aggregateId, _)))
  }

  private[scalasdk] def convertEvents(protobufEvents: Seq[Event]): Future[Seq[E]] = {
    Future.sequence(protobufEvents.map(convertEvent))
  }

  private[scalasdk] def convertState(aggregateId: String, maybeState: Option[S]): Future[Option[State]] = {
    maybeState match {
      case Some(state) =>
        Future
          .fromTry(serDeser.serializeState(state))
          .map((byteArray: Array[Byte]) =>
            Some {
              State(aggregateId, payload = ByteString.copyFrom(byteArray))
            })
      case None =>
        Future.successful(Option.empty[State])
    }
  }

  private[scalasdk] def toProcessCommandReply(aggregateId: String, result: Either[String, (Seq[E], Option[S])]): Future[ProcessCommandReply] = {
    result match {
      case Left(rejMsg: String) =>
        Future.successful(ProcessCommandReply(aggregateId, isSuccess = false, rejMsg))
      case Right(result: (Seq[E], Option[S])) =>
        for {
          events <- convertEvents(aggregateId, result._1)
          state <- convertState(aggregateId, result._2)
        } yield ProcessCommandReply(aggregateId, isSuccess = true, events = events, newState = state)
    }
  }

  override def processCommand(in: ProcessCommandRequest): Future[ProcessCommandReply] = {
    (in.state, in.command) match {
      case (maybePbState, Some(pbCommand)) =>
        val aggregateId: String = pbCommand.aggregateId
        val tryState: Try[Option[S]] = maybePbState match {
          case Some(pbState) =>
            serDeser.deserializeState(pbState.payload).map(Option(_))
          case None =>
            Success(Option.empty[S])
        }
        (for {
          state <- tryState
          cmd <- serDeser.deserializeCommand(pbCommand.payload)
          result <- Try(cqrsModel.executeCommand(state, cmd))
        } yield result) match {
          case Failure(exception) => Future.failed(exception)
          case Success(result: Either[String, (Seq[E], Option[S])]) =>
            toProcessCommandReply(aggregateId, result)
        }
      case (_, None) =>
        Future.failed(new Exception("No command provided in ProcessCommandRequest. This should never happen!"))
    }
  }

  override def handleEvents(in: HandleEventsRequest): Future[HandleEventsResponse] = {
    val tryState: Try[Option[S]] = in.state match {
      case Some(pbState) =>
        serDeser.deserializeState(pbState.payload).map(Option(_))
      case None =>
        Success(Option.empty[S])
    }
    for {
      oldState <- Future.fromTry(tryState)
      events <- convertEvents(in.events)
      newState <- Future.fromTry(Try(cqrsModel.applyEvents(oldState, events)))
      newStateProtobuf <- newState match {
        case Some(value: S) =>
          Future.fromTry(serDeser.serializeState(value).map(bytes => State(aggregateId = in.aggregateId, ByteString.copyFrom(bytes)))).map(Option(_))
        case None => Future.successful(Option.empty[State])
      }
      response = HandleEventsResponse(aggregateId = in.aggregateId, state = newStateProtobuf)
    } yield response
  }

}
