// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.cluster

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RemoteAddressExtensionSpec extends AnyWordSpec with Matchers {
  import RemoteActorSystems._

  "RemoteAddressExtension" should {
    "Be able to get the host/port of a system using artery for remoting" ignore {
      val address = RemoteAddressExtension(arteryConfiguredSystem).address
      address.host shouldEqual Some(arteryHost)
      address.port shouldEqual Some(arteryPort)
      address.protocol shouldEqual "akka"
      address.hasLocalScope shouldEqual false
      address.hasGlobalScope shouldEqual true
    }

    "Be able to get the host/port of a system using classic remoting" ignore {
      val address = RemoteAddressExtension(nettyConfiguredSystem).address
      address.host shouldEqual Some(nettyHost)
      address.port shouldEqual Some(nettyPort)
      address.protocol shouldEqual "akka.tcp"
      address.hasLocalScope shouldEqual false
      address.hasGlobalScope shouldEqual true
    }

    "Return None for host/port of a local actor system" ignore {
      val address = RemoteAddressExtension(localActorSystem).address
      address.host shouldEqual None
      address.port shouldEqual None
      address.protocol shouldEqual "akka"
      address.hasLocalScope shouldEqual true
      address.hasGlobalScope shouldEqual false
    }
  }
}
