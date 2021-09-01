// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example

import akka.actor.ActorSystem
import akka.event.Logging
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import com.ukg.surge.multilanguage.protobuf._
import com.ukg.surge.poc.business.{PersonTagged, Photo, TagPerson}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class BusinessServiceImpl(implicit system: ActorSystem) extends BusinessLogicService {

  val logger = Logging(system, classOf[BusinessServiceImpl])

  override def processCommand(in: ProcessCommandRequest): Future[ProcessCommandReply] = {
    logger.info("In processCommand")
    in.command match {
      case Some(command: Command) =>
        logger.info("Received command:" + command)
        val tagPerson: TagPerson = TagPerson.parseFrom(command.payload.toByteArray)
        val personTagged: PersonTagged = PersonTagged(personName = tagPerson.personName)
        val event = com.ukg.surge.multilanguage.protobuf.Event(command.aggregateId, personTagged.toByteString)
        logger.info(s"Responding with event with payload of size ${event.payload.size()}")
        Future.successful(ProcessCommandReply(List(event)))
      case None =>
        logger.info("No command! Returning failed future..")
        Future.failed(new UnsupportedOperationException)
    }
  }

  override def handleEvent(in: HandleEventRequest): Future[HandleEventResponse] = {
    in.state match {
      case Some(state) =>
        val photo = Photo.parseFrom(state.payload.toByteArray)
        in.event match {
          case Some(event) =>
            val personTagged = PersonTagged.parseFrom(event.toByteArray)
            val newState = State(photo.withNumPeople(photo.numPeople + 1).toByteString)
            val response = HandleEventResponse(Some(newState))
            Future.successful(response)
          case None =>
            Future.failed(new UnsupportedOperationException)
        }
      case None =>
        in.event match {
          case Some(event) =>
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

class Main

object Main {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("app")
    val logger = Logging(system, classOf[Main])

    import system.dispatcher
    val binding = new BusinessLogicServer(system).run()
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        binding.map(_.terminate(hardDeadline = 7.seconds))
      }
    })

    logger.info("Business logic server has been started")

    val config = system.settings.config.getConfig("surge-server")
    config.resolve()
    val host = config.getString("host")
    val port = config.getInt("port")
    logger.info(s"Surge host is $host")
    logger.info(s"Surge port is $port")

    lazy val clientSettings = GrpcClientSettings.connectToServiceAt(host, port).withTls(false)

    lazy val surge: MultilanguageGatewayService = MultilanguageGatewayServiceClient(clientSettings)

    val command =
      com.ukg.surge.multilanguage.protobuf.Command(aggregateId = java.util.UUID.randomUUID().toString, payload = TagPerson(personName = "Bob").toByteString)

    akka.pattern
      .after(duration = 15.seconds, system.scheduler) {
        surge.forwardCommand(ForwardCommandRequest(Some(command)))
      }
      .onComplete {
        case Failure(exception: Throwable) =>
          exception.printStackTrace()
        case Success(value: ForwardCommandReply) =>
          logger.info(s"Success ${value.toString}")
      }

  }
}

class BusinessLogicServer(system: ActorSystem) {
  def run(): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    val serverConfig = system.settings.config.getConfig("business-logic-server")
    val host = serverConfig.getString("host")
    val port = serverConfig.getInt("port")

    val service: HttpRequest => Future[HttpResponse] =
      BusinessLogicServiceHandler(new BusinessServiceImpl())

    val binding = Http().newServerAt(host, port).bind(service)

    binding.foreach { binding => println(s"gRPC server bound to: ${binding.localAddress}") }

    binding

  }
}
