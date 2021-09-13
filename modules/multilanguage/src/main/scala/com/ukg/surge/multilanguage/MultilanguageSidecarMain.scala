// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.Logging

class MultilanguageSidecarMain

object MultilanguageSidecarMain extends App {
  implicit val system = ActorSystem("multilanguage")
  val logger = Logging(system, classOf[MultilanguageSidecarMain])
  val binding = new MultilanguageGatewayServer(system).run()
}
