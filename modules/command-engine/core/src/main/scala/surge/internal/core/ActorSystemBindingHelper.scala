// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.core

import akka.actor.ActorSystem
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import org.slf4j.LoggerFactory

import java.net.InetAddress

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

  lazy val singletonActorSystem: ActorSystem = createActorSystem(s"SurgeActorSystem")
}

trait ActorSystemBindingHelper {
  def sharedActorSystem(): ActorSystem = ActorSystemBindingHelper.singletonActorSystem
}
