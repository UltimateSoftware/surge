// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka

import akka.actor.{ Actor, ActorRef, Props }
import com.ultimatesoftware.kafka.streams.{ HealthCheck, HealthCheckStatus, HealthyActor }
import com.ultimatesoftware.scala.core.kafka.{ HostPort, PartitionAssignments }
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

object KafkaConsumerStateTrackingActor {
  case class StateUpdated(newClusterState: Map[HostPort, List[TopicPartition]])
  case object GetPartitionAssignments
  case class Register(actor: ActorRef)

  case object Ack

  def props: Props = {
    Props(new KafkaConsumerStateTrackingActor)
  }
}

/**
 * The Kafka consumer state tracking actor is used to follow topic/partition assignment updates.
 * This actor must be updated with topic/partition assignments by something upstream, but provides
 * a single place where other interested actors can ask for updates and register to be notified if
 * the state of assignments changes at all.
 */
class KafkaConsumerStateTrackingActor extends Actor {
  import KafkaConsumerStateTrackingActor._
  private val log = LoggerFactory.getLogger(getClass)
  private case class ActorState(clusterState: Map[HostPort, List[TopicPartition]], registeredListeners: Set[ActorRef])

  override def receive: Receive = statefulReceive(ActorState(Map.empty, Set.empty))

  private def statefulReceive(actorState: ActorState): Receive = {
    case msg: StateUpdated       ⇒ handle(actorState, msg)
    case msg: Register           ⇒ registerActor(actorState, msg.actor)
    case GetPartitionAssignments ⇒ handleGetPartitionAssignments(actorState)
    case HealthyActor.GetHealth  ⇒ getHealthCheck(actorState)
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
    state.registeredListeners.foreach { listener ⇒
      sendPartitionAssignments(listener, state.clusterState)
    }
  }

  private def stringifyClusterState(clusterState: Map[HostPort, List[TopicPartition]]): String = {
    clusterState.map {
      case (hostPort, listTopicPartition) ⇒
        val topicsInOneLine = listTopicPartition.map(topic ⇒ topic.toString).mkString("", ", ", "")
        s"${hostPort.toString()} - $topicsInOneLine"
    }.mkString("", "; ", "")
  }
  private def getHealthCheck(actorState: ActorState) = {
    sender() ! HealthCheck(
      name = "partition-tracker-actor",
      id = s"partition-tracker-actor-${hashCode()}",
      status = HealthCheckStatus.UP,
      details = Some(Map(
        "state" -> stringifyClusterState(actorState.clusterState),
        "listeners" -> actorState.registeredListeners.size.toString)))
  }
}
