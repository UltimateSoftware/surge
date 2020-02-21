// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.cluster

import akka.actor.ActorSystem
import com.typesafe.config.{ Config, ConfigFactory }

object RemoteActorSystems {
  val arteryConfig: Config = ConfigFactory.load("artery-test-config.conf")
  val arteryConfiguredSystem: ActorSystem = ActorSystem("ArteryConfiguredSystem", arteryConfig)
  val arteryHost: String = arteryConfig.getString("akka.remote.artery.canonical.hostname")
  val arteryPort: Int = arteryConfig.getInt("akka.remote.artery.canonical.port")

  val nettyConfig: Config = ConfigFactory.load("classic-remoting-test-config.conf")
  val nettyConfiguredSystem: ActorSystem = ActorSystem("NettyConfiguredSystem", nettyConfig)
  val nettyHost: String = nettyConfig.getString("akka.remote.netty.tcp.hostname")
  val nettyPort: Int = nettyConfig.getInt("akka.remote.netty.tcp.port")

  val localConfig: Config = ConfigFactory.load("akka-local.conf")
  val localActorSystem: ActorSystem = ActorSystem("LocalActorSystem", localConfig)
}
