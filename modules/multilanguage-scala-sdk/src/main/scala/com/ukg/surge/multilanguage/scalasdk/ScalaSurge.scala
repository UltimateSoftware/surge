package com.ukg.surge.multilanguage.scalasdk

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import com.ukg.surge.multilanguage.protobuf.BusinessLogicServiceHandler

import scala.concurrent.{ExecutionContext, Future}

class ScalaSurgeServer[S, E,C](system: ActorSystem, CQRSModel: CQRSModel[S, E, C], serDeser: SerDeser[S, E, C]) {
  def run(): Future[Http.ServerBinding] = {
    // Akka boot up code
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    // Create service handlers
    val service: HttpRequest => Future[HttpResponse] =
      BusinessLogicServiceHandler(new BusinessServiceImpl(cqrsModel = CQRSModel, serDeser))

    // Bind service handler servers to localhost:8080/8081
    val binding = Http().newServerAt("127.0.0.1", 8080).bind(service)

    // report successful binding
    binding.foreach { binding => println(s"gRPC server bound to: ${binding.localAddress}") }

    binding
  }
}

class ScalaSurge[S, E, C](CQRSModel: CQRSModel[S, E, C], serDeser: SerDeser[S, E, C]) {
  def start() = {
    val system = ActorSystem()
    val server = new ScalaSurgeServer[S, E, C](system, CQRSModel, serDeser)
    server.run()

  }
}
