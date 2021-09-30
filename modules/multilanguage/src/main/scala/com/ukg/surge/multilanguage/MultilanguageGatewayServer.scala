// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import com.typesafe.config.ConfigFactory
import com.ukg.surge.multilanguage.protobuf.HealthCheckReply.Status
import com.ukg.surge.multilanguage.protobuf._
import surge.scaladsl.command.SurgeCommand

import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions

class MultilanguageGatewayServer(system: ActorSystem) {

  implicit val sys: ActorSystem = system
  implicit val ec: ExecutionContext = sys.dispatcher

  private val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServer])
  private val config = ConfigFactory.load()

  val aggregateName: String = config.getString("surge-server.aggregate-name")
  val eventsTopicName: String = config.getString("surge-server.events-topic")
  val stateTopicName: String = config.getString("surge-server.state-topic")

  val businessLogicgRPCHost: String = config.getString("business-logic-server.host")
  val businessLogicgRPCPort: Int = config.getInt("business-logic-server.port")
  val businessLogicgRPCClientSettings: GrpcClientSettings = GrpcClientSettings.connectToServiceAt(businessLogicgRPCHost, businessLogicgRPCPort).withTls(false)

  val bridgeToBusinessApp: BusinessLogicService = BusinessLogicServiceClient(businessLogicgRPCClientSettings)
  val genericSurgeCommandBusinessLogic = new GenericSurgeCommandBusinessLogic(aggregateName, eventsTopicName, stateTopicName, bridgeToBusinessApp)

  lazy val surgeEngine: SurgeCommand[UUID, SurgeState, SurgeCmd, Nothing, SurgeEvent] = {
    val engine = SurgeCommand(system, genericSurgeCommandBusinessLogic, system.settings.config)
    logger.info("Started engine!")
    engine.start()
    engine
  }

  def doHealthCheck(): Future[HealthCheckResponse] = {
    (for {
      surgeHealth <- surgeEngine.healthCheck
      businessAppHealth <- bridgeToBusinessApp.healthCheck(HealthCheckRequest()) if surgeHealth.isHealthy.getOrElse(false)
    } yield businessAppHealth.copy("multilanguage-server"))
      .recover { case _: Exception =>
        logger.warning("Multilanguage server is in DOWN state")
        HealthCheckReply(serviceName = "multilanguage-server", status = Status.DOWN)
      }
      .map(reply => HealthCheckResponse(reply.status.toString()))
  }

  def run(): Future[Http.ServerBinding] = {

    val service: HttpRequest => Future[HttpResponse] =
      MultilanguageGatewayServiceHandler(new MultilanguageGatewayServiceImpl(surgeEngine))

    val host = config.getString("surge-server.host")
    val port = config.getInt("surge-server.grpc-port")

    logger.info(
      s"Binding multilanguage gateway server on $host:$port. Aggregate name: $aggregateName. Events topic: $eventsTopicName. State topic: $stateTopicName")
    val binding = Http().newServerAt(host, port).bind(service)

    // report successful binding
    binding.foreach { binding => logger.info(s"gRPC server bound to: ${binding.localAddress}") }

    binding

  }
}
