// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.kafka

import akka.actor.{ Actor, ActorSystem, BootstrapSetup, Props, ProviderSelection }
import akka.pattern._
import akka.util.Timeout
import com.ultimatesoftware.config.TimeoutConfig
import com.ultimatesoftware.scala.core.kafka.HostPort
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

object HostAssignmentTracker {
  private val log = LoggerFactory.getLogger(getClass)
  private val system = ActorSystem.create("hostAssignmentSystem", BootstrapSetup().withActorRefProvider(ProviderSelection.local()))

  private val underlyingActor = system.actorOf(Props(new HostAssignmentTrackerImpl))
  private implicit val actorAskTimeout: Timeout = Timeout(TimeoutConfig.PartitionTracker.updateTimeout)

  def updateState(metadata: HostPort, partitions: List[TopicPartition]): Unit = {
    underlyingActor ! UpdateState(metadata, partitions)
  }

  def getAssignment(topicPartition: TopicPartition): Future[Option[HostPort]] = {
    (underlyingActor ? GetAssignment(topicPartition)).mapTo[Option[HostPort]]
  }

  def allAssignments(implicit ec: ExecutionContext): Future[Map[TopicPartition, HostPort]] = {
    (underlyingActor ? GetState).mapTo[ClusterState].map(_.state)
  }

  private case class UpdateState(hostPort: HostPort, partitions: List[TopicPartition])
  private case object GetState
  private case class GetAssignment(topicPartition: TopicPartition)

  private case class ClusterState(state: Map[TopicPartition, HostPort])
  private class HostAssignmentTrackerImpl extends Actor {
    override def receive: Receive = receiveWithState(ClusterState(Map.empty))

    private def receiveWithState(state: ClusterState): Receive = {
      case msg: UpdateState   ⇒ handleUpdateState(state, msg)
      case GetState           ⇒ sender() ! state
      case msg: GetAssignment ⇒ sender() ! state.state.get(msg.topicPartition)
    }

    private def handleUpdateState(clusterState: ClusterState, update: UpdateState): Unit = {
      val newStates = update.partitions.map { partition ⇒
        // TODO JEFF change info log to trace when done testing locally
        log.info("Host {} is now responsible for partition {}", Seq(update.hostPort, partition): _*)
        partition -> update.hostPort
      }

      val newStateMap = clusterState.state ++ newStates.toMap
      val newClusterState = clusterState.copy(state = newStateMap)
      context.become(receiveWithState(newClusterState))
    }
  }
}
