// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package com.ukg.surge.multilanguage.scalasdk

import akka.actor.ActorSystem
import akka.event.Logging
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import com.google.protobuf.ByteString
import com.ukg.surge.multilanguage.protobuf.{ BusinessLogicServiceHandler, Command, ForwardCommandRequest, MultilanguageGatewayServiceClient }

import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }

class ScalaSurgeServer[S, E, C](system: ActorSystem, CQRSModel: CQRSModel[S, E, C], serDeser: SerDeser[S, E, C]) {

  implicit val sys: ActorSystem = system
  implicit val ec: ExecutionContext = sys.dispatcher

  val logger = Logging(sys, "ScalaSurgeServer")

  val config = system.settings.config.getConfig("business-logic-server")
  val host = config.getString("host")
  val port = config.getInt("port")

  val surgeServerConfig = system.settings.config.getConfig("surge-server")
  val surgeHost = surgeServerConfig.getString("host")
  val surgePort = surgeServerConfig.getInt("port")

  logger.info(s"The multilanguage server is running at ${surgeHost}:${surgePort}!")

  val service: HttpRequest => Future[HttpResponse] =
    BusinessLogicServiceHandler(new BusinessServiceImpl(cqrsModel = CQRSModel, serDeser))

  val binding = Http().newServerAt(host, port).bind(service)

  // report successful binding
  binding.foreach { binding => logger.info(s"gRPC of business logic server bound to: ${binding.localAddress}") }

  val surgeClientSettings: GrpcClientSettings = GrpcClientSettings.connectToServiceAt(surgeHost, surgePort)
  val bridgeToSurge = MultilanguageGatewayServiceClient(surgeClientSettings)

  def forwardCommand(aggregateId: UUID, cmd: C): Future[Option[S]] = {
    val tryPbCommand = serDeser.serializeCommand(cmd)
    (for {
      pbCmdByteArray <- Future.fromTry(tryPbCommand)
      pbCmd = Command(aggregateId.toString, payload = ByteString.copyFrom(pbCmdByteArray))
      request = ForwardCommandRequest(aggregateId.toString, command = Some(pbCmd))
      response <- bridgeToSurge.forwardCommand(request)
      state <- response.newState match {
        case Some(pbState) => Future.fromTry(serDeser.deserializeState(pbState.payload)).map(Some(_))
        case None          => Future.successful(Option.empty[S])
      } if response.isSuccess
    } yield state)
  }

}

class ScalaSurge[S, E, C](CQRSModel: CQRSModel[S, E, C], serDeser: SerDeser[S, E, C]) {
  def start(): Unit = {
    val system = ActorSystem()
    val server = new ScalaSurgeServer[S, E, C](system, CQRSModel, serDeser)
  }
}
