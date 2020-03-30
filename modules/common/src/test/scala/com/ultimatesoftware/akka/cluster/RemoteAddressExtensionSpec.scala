// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.cluster

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RemoteAddressExtensionSpec extends AnyWordSpec with Matchers {
  import RemoteActorSystems._

  "RemoteAddressExtension" should {
    "Be able to get the host/port of a system using artery for remoting" in {
      val address = RemoteAddressExtension(arteryConfiguredSystem).address
      address.host shouldEqual Some(arteryHost)
      address.port shouldEqual Some(arteryPort)
      address.protocol shouldEqual "akka"
      address.hasLocalScope shouldEqual false
      address.hasGlobalScope shouldEqual true
    }

    "Be able to get the host/port of a system using classic remoting" in {
      val address = RemoteAddressExtension(nettyConfiguredSystem).address
      address.host shouldEqual Some(nettyHost)
      address.port shouldEqual Some(nettyPort)
      address.protocol shouldEqual "akka.tcp"
      address.hasLocalScope shouldEqual false
      address.hasGlobalScope shouldEqual true
    }

    "Return None for host/port of a local actor system" in {
      val address = RemoteAddressExtension(localActorSystem).address
      address.host shouldEqual None
      address.port shouldEqual None
      address.protocol shouldEqual "akka"
      address.hasLocalScope shouldEqual true
      address.hasGlobalScope shouldEqual false
    }
  }
}
