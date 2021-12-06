// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.kafka

import akka.actor.{ Actor, ActorRef, ActorSelection, NoSerializationVerificationNeeded, Props }
import akka.pattern.ask
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import surge.core.ControllableAdapter
import surge.internal.config.TimeoutConfig
import surge.kafka.streams.{ HealthCheck, HealthCheckStatus, HealthyActor, HealthyComponent }
import surge.kafka.{ HostPort, PartitionAssignments }

import scala.concurrent.Future

class KafkaConsumerPartitionAssignmentTracker(val underlyingActor: ActorRef) extends ControllableAdapter with HealthyComponent {
  def register(implicit actorRef: ActorRef): Unit = {
    underlyingActor ! KafkaConsumerStateTrackingActor.Register(actorRef)
  }

  def getPartitionAssignments(implicit timeout: Timeout): Future[PartitionAssignments] = {
    underlyingActor.ask(KafkaConsumerStateTrackingActor.GetPartitionAssignments).mapTo[PartitionAssignments]
  }

  override def healthCheck(): Future[HealthCheck] = {
    underlyingActor.ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout).mapTo[HealthCheck]
  }
}

class KafkaConsumerPartitionAssignmentTrackerWithSelection(val underlyingActor: ActorSelection) {
  def getPartitionAssignments(implicit timeout: Timeout): Future[PartitionAssignments] = {
    underlyingActor.ask(KafkaConsumerStateTrackingActor.GetPartitionAssignments).mapTo[PartitionAssignments]
  }
}

object KafkaConsumerStateTrackingActor {
  sealed trait Message extends NoSerializationVerificationNeeded // These should only ever be locally sent
  case class StateUpdated(newClusterState: Map[HostPort, List[TopicPartition]]) extends Message
  case object GetPartitionAssignments extends Message
  case class Register(actor: ActorRef) extends Message
  case object Ack extends Message

  def props: Props = {
    Props(new KafkaConsumerStateTrackingActor)
  }
}

/**
 * The Kafka consumer state tracking actor is used to follow topic/partition assignment updates. This actor must be updated with topic/partition assignments by
 * something upstream, but provides a single place where other interested actors can ask for updates and register to be notified if the state of assignments
 * changes at all.
 */
class KafkaConsumerStateTrackingActor extends Actor {
  import KafkaConsumerStateTrackingActor._
  private val log = LoggerFactory.getLogger(getClass)
  private case class ActorState(clusterState: Map[HostPort, List[TopicPartition]], registeredListeners: Set[ActorRef])

  override def receive: Receive = statefulReceive(ActorState(Map.empty, Set.empty))

  private def statefulReceive(actorState: ActorState): Receive = {
    case msg: StateUpdated       => handle(actorState, msg)
    case msg: Register           => registerActor(actorState, msg.actor)
    case GetPartitionAssignments => handleGetPartitionAssignments(actorState)
    case HealthyActor.GetHealth  => doHealthCheck(actorState)
  }

  private def handle(actorState: ActorState, stateUpdated: StateUpdated): Unit = {
    val newState = actorState.copy(clusterState = stateUpdated.newClusterState)
    log.info(s"Updating state to ${stateUpdated.newClusterState}")
    context.become(statefulReceive(newState))
    updateListeners(newState)
    sender() ! Ack
  }

  private def sendPartitionAssignments(recipient: ActorRef, clusterState: Map[HostPort, List[TopicPartition]]): Unit = {
    log.info(s"Sending partition assignments to ${recipient.path}")
    recipient ! PartitionAssignments(clusterState)
  }

  private def registerActor(state: ActorState, registeredActor: ActorRef): Unit = {
    log.info(s"ClusterStateTrackingActor registering listener ${registeredActor.path}")

    val newState = state.copy(registeredListeners = state.registeredListeners + registeredActor)
    context.become(statefulReceive(newState))

    sendPartitionAssignments(registeredActor, state.clusterState)
  }

  private def handleGetPartitionAssignments(state: ActorState): Unit = {
    sendPartitionAssignments(sender(), state.clusterState)
  }

  private def updateListeners(state: ActorState): Unit = {
    state.registeredListeners.foreach { listener =>
      sendPartitionAssignments(listener, state.clusterState)
    }
  }

  private def stringifyClusterState(clusterState: Map[HostPort, List[TopicPartition]]): String = {
    clusterState
      .map { case (hostPort, listTopicPartition) =>
        val topicsInOneLine = listTopicPartition.map(topic => topic.toString).mkString("", ", ", "")
        s"${hostPort.toString()} - $topicsInOneLine"
      }
      .mkString("", "; ", "")
  }

  private def doHealthCheck(actorState: ActorState): Unit = {
    sender() ! HealthCheck(
      name = "partition-tracker-actor",
      id = s"partition-tracker-actor-${hashCode()}",
      status = HealthCheckStatus.UP,
      details = Some(Map("state" -> stringifyClusterState(actorState.clusterState), "listeners" -> actorState.registeredListeners.size.toString)))
  }
}
