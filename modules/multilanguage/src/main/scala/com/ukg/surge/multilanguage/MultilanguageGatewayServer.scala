// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import com.typesafe.config.{ Config, ConfigFactory }
import com.ukg.surge.multilanguage.protobuf._

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions

class MultilanguageGatewayServer(system: ActorSystem) {

  def run(): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServer])

    val config = ConfigFactory.load()
    val host = config.getString("surge-server.host")
    val port = config.getInt("surge-server.port")

    val aggregateName: String = config.getString("surge-server.aggregate-name")
    val eventsTopicName: String = config.getString("surge-server.events-topic")
    val stateTopicName: String = config.getString("surge-server.state-topic")

    val businessLogicgRPCHost: String = config.getString("business-logic-server.host")
    val businessLogicgRPCPort: Int = config.getInt("business-logic-server.port")
    val businessLogicgRPCClientSettings: GrpcClientSettings = GrpcClientSettings.connectToServiceAt(businessLogicgRPCHost, businessLogicgRPCPort).withTls(false)

    val bridgeToBusinessApp: BusinessLogicService = BusinessLogicServiceClient(businessLogicgRPCClientSettings)

    logger.info(
      s"Binding multilanguage gateway server on $host:$port. Aggregate name: $aggregateName. Events topic: $eventsTopicName. State topic: $stateTopicName")

    val service: HttpRequest => Future[HttpResponse] =
      MultilanguageGatewayServiceHandler(new MultilanguageGatewayServiceImpl(bridgeToBusinessApp, aggregateName, eventsTopicName, stateTopicName))

    val binding = Http().newServerAt(host, port).bind(service)

    // report successful binding
    binding.foreach { binding => logger.info(s"gRPC server bound to: ${binding.localAddress}") }

    binding

  }
}
