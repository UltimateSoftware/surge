// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import akka.stream.Materializer
import com.typesafe.config.ConfigFactory
import com.ukg.surge.poc._

import scala.concurrent.{ ExecutionContext, Future }

class BusinessServiceImpl(implicit mat: Materializer) extends BusinessLogicService {

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

object BusinessLogicServer {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseString("akka.http.server.preview.enable-http2 = on").withFallback(ConfigFactory.defaultApplication())
    val system = ActorSystem("net", conf)
    new BusinessLogicServer(system).run()
  }
}

class BusinessLogicServer(system: ActorSystem) {
  def run(): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    val service: HttpRequest => Future[HttpResponse] =
      BusinessLogicServiceHandler(new BusinessServiceImpl())

    val binding = Http().newServerAt("127.0.0.1", 8080).bind(service)

    binding.foreach { binding => println(s"gRPC server bound to: ${binding.localAddress}") }

    binding
  }
}
