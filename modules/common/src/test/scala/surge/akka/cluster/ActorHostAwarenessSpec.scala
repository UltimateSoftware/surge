// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.akka.cluster

import akka.actor.ActorSystem
import org.apache.kafka.streams.state.HostInfo
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ActorHostAwarenessSpec extends AnyWordSpec with Matchers {
  import RemoteActorSystems._
  trait LocalAwareness extends ActorSystemHostAwareness {
    override def actorSystem: ActorSystem = localActorSystem
  }
  trait ArteryAwareness extends ActorSystemHostAwareness {
    override def actorSystem: ActorSystem = arteryConfiguredSystem
  }

  "ActorHostAwareness" should {
    "Return None for an application host/port of a local actor system" in new LocalAwareness {
      applicationHostPort shouldEqual None
    }
    "Return the configured host/port for a system configured with artery" in new ArteryAwareness {
      applicationHostPort shouldEqual Some(s"$arteryHost:$arteryPort")
    }
    "Assume HostInfo is local for a locally configured actor system" in new LocalAwareness {
      val hostInfo = new HostInfo("doesntMatter", 1)
      isHostInfoThisNode(hostInfo) shouldEqual true
    }
    "Be able to determine if HostInfo is local based on host/port info for an actor system configured with remoting" in new ArteryAwareness {
      val localHostInfo = new HostInfo(arteryHost, arteryPort)
      val nonLocalHostInfo = new HostInfo("other", 1)

      isHostInfoThisNode(localHostInfo) shouldEqual true
      isHostInfoThisNode(nonLocalHostInfo) shouldEqual false
      akkaProtocol shouldEqual "akka"
    }
  }
}
