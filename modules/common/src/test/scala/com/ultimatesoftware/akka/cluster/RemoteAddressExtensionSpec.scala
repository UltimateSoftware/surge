// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.cluster

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.scalatest.{ Matchers, WordSpec }

class RemoteAddressExtensionSpec extends WordSpec with Matchers {
  "RemoteAddressExtension" should {
    "Be able to get the host/port of a system using artery for remoting" in {
      val config = ConfigFactory.load("artery-test-config.conf")
      val testSystem = ActorSystem("ArteryConfiguredSystem", config)
      val expectedHost = config.getString("akka.remote.artery.canonical.hostname")
      val expectedPort = config.getInt("akka.remote.artery.canonical.port")

      val address = RemoteAddressExtension(testSystem).address
      address.host shouldEqual Some(expectedHost)
      address.port shouldEqual Some(expectedPort)
      address.protocol shouldEqual "akka"
      address.hasLocalScope shouldEqual false
      address.hasGlobalScope shouldEqual true
    }

    "Be able to get the host/port of a system using classic remoting" in {
      val config = ConfigFactory.load("classic-remoting-test-config.conf")
      val testSystem = ActorSystem("NettyConfiguredSystem", config)
      val expectedHost = config.getString("akka.remote.netty.tcp.hostname")
      val expectedPort = config.getInt("akka.remote.netty.tcp.port")

      val address = RemoteAddressExtension(testSystem).address
      address.host shouldEqual Some(expectedHost)
      address.port shouldEqual Some(expectedPort)
      address.protocol shouldEqual "akka.tcp"
      address.hasLocalScope shouldEqual false
      address.hasGlobalScope shouldEqual true
    }

    "Return None for host/port of a local actor system" in {
      val config = ConfigFactory.load("akka-local.conf")
      val testSystem = ActorSystem("LocalActorSystem", config)

      val address = RemoteAddressExtension(testSystem).address
      address.host shouldEqual None
      address.port shouldEqual None
      address.protocol shouldEqual "akka"
      address.hasLocalScope shouldEqual true
      address.hasGlobalScope shouldEqual false
    }
  }
}
