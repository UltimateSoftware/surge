// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import com.ukg.surge.multilanguage.protobuf._

import java.net.InetAddress
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class MultilanguageGatewayServer(system: ActorSystem) {

  def run(): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServer])

    val kubernetesNamespace = "kafka"
    val podAddress = InetAddress.getLocalHost.getHostAddress.replace(".", "-")
    val podAddressCorrect = s"$podAddress.$kubernetesNamespace.pod.cluster.local"

    val config = system.settings.config.getConfig("surge-server")
    val host = podAddressCorrect
    val port = config.getInt("port")

    logger.info(s"Binding multilanguage gateway server on ${host}:${port}")

    val service: HttpRequest => Future[HttpResponse] =
      MultilanguageGatewayServiceHandler(new MultilanguageGatewayServiceImpl())

    val binding = Http().newServerAt(host, port).bind(service)

    // report successful binding
    binding.foreach { binding => logger.info(s"gRPC server bound to: ${binding.localAddress}") }

    binding

  }
}
