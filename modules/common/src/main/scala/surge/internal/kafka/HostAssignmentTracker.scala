// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import akka.actor.{ Actor, ActorSystem, BootstrapSetup, Props, ProviderSelection }
import akka.pattern._
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import surge.internal.config.TimeoutConfig
import surge.kafka.HostPort

import scala.concurrent.{ ExecutionContext, Future }

object HostAssignmentTracker {
  private val log = LoggerFactory.getLogger(getClass)
  private val system = ActorSystem.create("hostAssignmentSystem", BootstrapSetup().withActorRefProvider(ProviderSelection.local()))

  private val underlyingActor = system.actorOf(Props(new HostAssignmentTrackerImpl))
  private implicit val actorAskTimeout: Timeout = Timeout(TimeoutConfig.PartitionTracker.updateTimeout)

  def updateState(stateMap: Map[TopicPartition, HostPort]): Unit = {
    stateMap.foreach(tup => updateState(tup._1, tup._2))
  }
  def updateState(partition: TopicPartition, hostPort: HostPort): Unit = {
    underlyingActor ! UpdateState(partition, hostPort)
  }

  def getAssignment(topicPartition: TopicPartition): Future[Option[HostPort]] = {
    (underlyingActor ? GetAssignment(topicPartition)).mapTo[Option[HostPort]]
  }

  def allAssignments(implicit ec: ExecutionContext): Future[Map[TopicPartition, HostPort]] = {
    (underlyingActor ? GetState).mapTo[ClusterState].map(_.state)
  }

  def getAssignments(topicName: String)(implicit ec: ExecutionContext): Future[Map[TopicPartition, HostPort]] = {
    allAssignments.map { assignments =>
      assignments.filter { case (topicPartition, _) =>
        topicPartition.topic().equals(topicName)
      }
    }
  }

  def clearAllAssignments(): Unit = {
    underlyingActor ! ClearState
  }

  private case class UpdateState(partition: TopicPartition, hostPort: HostPort)
  private case object GetState
  private case class GetAssignment(topicPartition: TopicPartition)
  private case object ClearState

  private case class ClusterState(state: Map[TopicPartition, HostPort])
  private class HostAssignmentTrackerImpl extends Actor {
    override def receive: Receive = receiveWithState(ClusterState(Map.empty))

    private def receiveWithState(state: ClusterState): Receive = {
      case msg: UpdateState   => handleUpdateState(state, msg)
      case GetState           => sender() ! state
      case msg: GetAssignment => sender() ! state.state.get(msg.topicPartition)
      case ClearState         => context.become(receiveWithState(ClusterState(Map.empty)))
    }

    private def handleUpdateState(clusterState: ClusterState, update: UpdateState): Unit = {
      log.trace("Host {} is now responsible for partition {}", Seq(update.hostPort, update.partition): _*)

      val newStateMap = clusterState.state + (update.partition -> update.hostPort)
      val newClusterState = clusterState.copy(state = newStateMap)
      context.become(receiveWithState(newClusterState))
    }
  }
}
