// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.Logging
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.slf4j.LoggerFactory

import java.net.InetAddress
import scala.concurrent.duration._

class MultilanguageSidecarMain

object ActorSystemBindingHelper {

  private val log = LoggerFactory.getLogger(getClass)

  private val config = ConfigFactory.load()


  private def getHostname(configPath: String): String = {
    val configHostname = config.getString(configPath)
    configHostname match {
      case "<getKubernetesPodAddress>" =>
        val kubernetesNamespace = config.getString("surge.kubernetes.namespace")
        // Kubernetes pods are named based on their IP address but with dashes
        val podAddress = InetAddress.getLocalHost.getHostAddress.replace(".", "-")
        s"$podAddress.$kubernetesNamespace.pod.cluster.local"
      case _ =>
        configHostname
    }
  }

  def createActorSystem(name: String): ActorSystem = {
    val advertisedHostname = getHostname("surge.advertised.hostname")
    val bindHostname = getHostname("surge.bind.hostname")
    log.info("Setting advertised hostname to {}", advertisedHostname)
    val configOverride = config
      .withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef(advertisedHostname))
      .withValue("akka.remote.artery.bind.hostname", ConfigValueFactory.fromAnyRef(bindHostname))
    ActorSystem.create(name, configOverride)
  }

}


object MultilanguageSidecarMain extends App {
  implicit val system = ActorSystemBindingHelper.createActorSystem("multilanguage")
  val logger = Logging(system, classOf[MultilanguageSidecarMain])
  import system.dispatcher
  val binding = new MultilanguageGatewayServer(system).run()
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      binding.map(_.terminate(hardDeadline = 7.seconds))
    }
  })
}
