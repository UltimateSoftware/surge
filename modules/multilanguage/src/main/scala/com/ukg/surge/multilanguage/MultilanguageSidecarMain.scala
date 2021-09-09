// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.Logging

import scala.concurrent.duration._

class MultilanguageSidecarMain

object MultilanguageSidecarMain extends App {
  implicit val system = ActorSystem("multilanguage")
  val logger = Logging(system, classOf[MultilanguageSidecarMain])
  import system.dispatcher
  val binding = new MultilanguageGatewayServer(system).run()
}
