// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.server.Directives._
import com.ukg.surge.multilanguage.protobuf.HealthCheckReply
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import play.api.libs.json.{ Format, Json }

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

class MultilanguageSidecarMain

object MultilanguageSidecarMain extends App with PlayJsonSupport {

  import Implicits._

  implicit val system = ActorSystem("multilanguage")
  implicit val ec: ExecutionContext = system.dispatcher

  val logger = Logging(system, classOf[MultilanguageSidecarMain])
  val multilanguageServer = new MultilanguageGatewayServer(system)

  val route = path("healthcheck") {
    get {
      onComplete(multilanguageServer.doHealthCheck()) {
        case Success(value) => complete(value)
        case Failure(ex)    => complete(InternalServerError, s"An error occurred: ${ex.getMessage}\n")
      }
    }
  }

  val host = system.settings.config.getString("surge-server.host")
  val port = system.settings.config.getInt("surge-server.http-port")

  val binding = multilanguageServer.run()

  val bindingFuture = Http().newServerAt(host, port).bind(route)
  bindingFuture.foreach { binding => logger.info(s"REST API server bound to: ${binding.localAddress}") }

}
