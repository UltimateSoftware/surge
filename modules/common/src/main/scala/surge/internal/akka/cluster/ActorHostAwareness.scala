// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.cluster

import akka.actor.{ ActorContext, ActorPath, ActorSystem, Address }
import org.apache.kafka.streams.state.HostInfo
import surge.kafka.HostPort

trait ActorSystemHostAwareness {
  def actorSystem: ActorSystem

  protected lazy val localAddress: Address = RemoteAddressExtension(actorSystem).address
  protected lazy val localHostname: String = localAddress.host.getOrElse("localhost")
  protected lazy val localPort: Int = localAddress.port.getOrElse(0)
  protected lazy val akkaProtocol: String = localAddress.protocol
  protected lazy val localHostPort: HostPort = HostPort(localHostname, localPort)

  protected lazy val applicationHostPort: Option[String] = for {
    akkaHost <- localAddress.host
    akkaPort <- localAddress.port
  } yield {
    s"$akkaHost:$akkaPort"
  }

  protected def isHostPortThisNode(hostPort: HostPort): Boolean = {
    val hostPortsMatch = hostPort.host == localHostname && hostPort.port == localPort
    localAddress.hasLocalScope || hostPortsMatch
  }

  protected def isAddressThisNode(address: Address): Boolean = {
    val hostPortsMatch = address.host.contains(localHostname) && address.port.contains(localPort)
    localAddress.hasLocalScope || hostPortsMatch
  }

  protected def isHostInfoThisNode(hostInfo: HostInfo): Boolean = {
    val hostPort = HostPort(hostInfo.host(), hostInfo.port())
    isHostPortThisNode(hostPort)
  }

  protected def remotePath(path: String, remoteAddress: Address): String = {
    ActorPath.fromString(path).toStringWithAddress(remoteAddress)
  }

  protected def remotePath(path: String, hostPort: HostPort): String = {
    remotePath(path, hostPort.toAddress)
  }

  implicit class HostPortToActorAddress(hostPort: HostPort) {
    def toAddress: Address = {
      Address(akkaProtocol, actorSystem.name, hostPort.host, hostPort.port)
    }
  }
}

trait ActorHostAwareness extends ActorSystemHostAwareness {
  implicit def context: ActorContext

  override def actorSystem: ActorSystem = context.system
}
