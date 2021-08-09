// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example

import com.ukg.surge.poc.{
  BusinessLogicService,
  Command,
  Event,
  HandleEventRequest,
  HandleEventResponse,
  PersonTagged,
  Photo,
  ProcessCommandReply,
  ProcessCommandRequest,
  State,
  TagPerson
}

import scala.concurrent.Future

class BusinessServiceImpl extends BusinessLogicService {

  override def processCommand(in: ProcessCommandRequest): Future[ProcessCommandReply] = {
    in.command match {
      case Some(command: Command) =>
        val tagPerson: TagPerson = com.ukg.surge.poc.TagPerson.parseFrom(command.payload.toByteArray)
        val personTagged: PersonTagged = PersonTagged(personName = tagPerson.personName)
        val events = com.ukg.surge.poc.Event(command.aggregateId, personTagged.toByteString)
        Future.successful(ProcessCommandReply(List(events)))
      case None =>
        Future.failed(new UnsupportedOperationException)
    }
  }

  override def handleEvent(in: HandleEventRequest): Future[HandleEventResponse] = {
    in.state match {
      case Some(state: State) =>
        val photo = Photo.parseFrom(state.payload.toByteArray)
        in.event match {
          case Some(event: Event) =>
            val personTagged = PersonTagged.parseFrom(event.toByteArray)
            val newState = State(photo.withNumPeople(photo.numPeople + 1).toByteString)
            val response = HandleEventResponse(Some(newState))
            Future.successful(response)
          case None =>
            Future.failed(new UnsupportedOperationException)
        }
      case None =>
        in.event match {
          case Some(event: Event) =>
            val personTagged = PersonTagged.parseFrom(event.toByteArray)
            val newState = State(Photo(caption = "The only picture", numPeople = 1).toByteString)
            val response = HandleEventResponse(Some(newState))
            Future.successful(response)
          case None =>
            Future.failed(new UnsupportedOperationException)
        }
    }
  }

}

object BusinessAppMain extends App {}
