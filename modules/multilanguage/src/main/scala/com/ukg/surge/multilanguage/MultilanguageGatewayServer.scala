// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import com.typesafe.config.Config
import com.ukg.surge.multilanguage.protobuf._

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions

class MultilanguageGatewayServer(system: ActorSystem) {

  def run(): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServer])

    val config = system.settings.config.getConfig("surge-server")
    val host = config.getString("host")
    val port = config.getInt("port")

    val aggregateName: String = config.getString("aggregate-name")
    val eventsTopicName: String = config.getString("events-topic")
    val stateTopicName: String = config.getString("state-topic")

    logger.info(
      s"Binding multilanguage gateway server on $host:$port. Aggregate name: $aggregateName. Events topic: $eventsTopicName. State topic: $stateTopicName")

    val service: HttpRequest => Future[HttpResponse] =
      MultilanguageGatewayServiceHandler(new MultilanguageGatewayServiceImpl(aggregateName, eventsTopicName, stateTopicName))

    val binding = Http().newServerAt(host, port).bind(service)

    // report successful binding
    binding.foreach { binding => logger.info(s"gRPC server bound to: ${binding.localAddress}") }

    binding

  }
}
