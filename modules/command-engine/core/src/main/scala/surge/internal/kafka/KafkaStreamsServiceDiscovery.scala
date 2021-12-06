// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import akka.actor.ExtendedActorSystem
import akka.discovery.ServiceDiscovery.ResolvedTarget
import akka.discovery.{ Lookup, ServiceDiscovery }
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTrackerWithSelection

import java.net.InetAddress
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

final class KafkaStreamsServiceDiscovery(actorSystem: ExtendedActorSystem) extends ServiceDiscovery {
  import actorSystem.dispatcher

  private val stateChangeActor = actorSystem.actorSelection("user/state-change-actor")
  private val partitionTracker = new KafkaConsumerPartitionAssignmentTrackerWithSelection(stateChangeActor)

  override def lookup(lookup: Lookup, resolveTimeout: FiniteDuration): Future[ServiceDiscovery.Resolved] = {
    val result = for {
      partitionAssignment <- partitionTracker.getPartitionAssignments(resolveTimeout)
      seedNodes = partitionAssignment.partitionAssignments.keys.map { hostPort =>
        ResolvedTarget(hostPort.host, None, Try(InetAddress.getByName(hostPort.host)).toOption)
      }
    } yield seedNodes

    result.map { resolvedTargets =>
      ServiceDiscovery.Resolved(lookup.serviceName, resolvedTargets.toList)
    }
  }

}
