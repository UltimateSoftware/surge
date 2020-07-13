// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.kafka

import akka.actor.{ Actor, ActorRef, Address }
import com.ultimatesoftware.akka.cluster.{ ActorHostAwareness, ActorRegistry }
import com.ultimatesoftware.akka.streams.kafka.KafkaStreamManagerActor.{ StartConsuming, SuccessfullyStopped }
import com.ultimatesoftware.kafka.streams.core.EventReplaySource
import com.ultimatesoftware.scala.core.kafka.{ HostPort, KafkaTopic }
import org.apache.kafka.common.TopicPartition
import akka.pattern.pipe
import com.ultimatesoftware.support.inlineReceive
import scala.concurrent.Future
import scala.util.{ Failure, Success }

private[streams] object ReplayCoordinator {
  sealed trait ReplayCoordinatorCommand
  case object StartReplay extends ReplayCoordinatorCommand
  case object StopReplay extends ReplayCoordinatorCommand
  case object DoPreReplay extends ReplayCoordinatorCommand

  sealed trait ReplayCoordinatorEvent
  case class TopicConsumersFound(assignments: Map[TopicPartition, HostPort], actorPaths: List[String]) extends ReplayCoordinatorEvent
  case class TopicAssignmentsFound(assignments: Map[TopicPartition, HostPort]) extends ReplayCoordinatorEvent
  case object ReplayCompleted extends ReplayCoordinatorEvent
  case object PreReplayCompleted extends ReplayCoordinatorEvent
  case class ReplayFailed(reason: Throwable) extends ReplayCoordinatorEvent

  case class ReplayState(replyTo: ActorRef, running: Set[String], stopped: Set[String], assignments: Map[TopicPartition, HostPort])
  object ReplayState {
    def init(sender: ActorRef): ReplayState = ReplayState(sender, Set(), Set(), Map())
  }
}

class ReplayCoordinator[Key, Value](
    topicName: String,
    consumerGroup: String,
    replaySource: EventReplaySource[Key, Value]) extends Actor with ActorHostAwareness with ActorRegistry {

  import com.ultimatesoftware.akka.streams.kafka.ReplayCoordinator._

  import context.dispatcher

  override def receive: Receive = uninitialized()

  def uninitialized(): Receive = {
    case StartReplay ⇒
      context.become(ready(ReplayState.init(sender())))
      getTopicAssignments().map(assignments ⇒ TopicAssignmentsFound(assignments)).pipeTo(self)
  }

  def ready(replayState: ReplayState): Receive = inlineReceive {
    case TopicAssignmentsFound(assignments) ⇒
      getTopicConsumers(assignments).map(actors ⇒ TopicConsumersFound(assignments, actors)).pipeTo(self)
    case TopicConsumersFound(topicAssignments, topicConsumerActors) ⇒
      val waitingActorsPaths = topicConsumerActors.toSet
      context.become(pausing(replayState.copy(running = waitingActorsPaths, stopped = Set.empty, assignments = topicAssignments)))
      topicConsumerActors.foreach { streamManagerActorPath ⇒
        actorSystem.actorSelection(streamManagerActorPath) ! KafkaStreamManagerActor.StopConsuming
      }
  } orElse handleStopReplay(replayState)

  def pausing(replayState: ReplayState): Receive = inlineReceive {
    case SuccessfullyStopped(address, ref) ⇒
      handleSuccessfullyStopped(replayState, address, ref)
    case PreReplayCompleted ⇒
      doReplay(replayState)
    case DoPreReplay ⇒
      doPreReplay()
  } orElse handleStopReplay(replayState)

  def replaying(replayState: ReplayState): Receive = inlineReceive {
    case failure: ReplayFailed ⇒
      startStoppedConsumers(replayState)
      replayState.replyTo ! failure
      context.become(uninitialized())
    case ReplayCompleted ⇒
      replaySource.postReplay()
      startStoppedConsumers(replayState)
      replayState.replyTo ! ReplayCompleted
      context.become(uninitialized())
  } orElse handleStopReplay(replayState)

  def handleStopReplay(replayState: ReplayState): Receive = {
    case ReplayCoordinator.StopReplay ⇒
      log.debug("StopReplay received, this is typically because a timeout of the ReplayCoordinator or an unexpected error")
      startStoppedConsumers(replayState)
      context.become(uninitialized())
  }

  def handleSuccessfullyStopped(replayState: ReplayState, address: Address, ref: ActorRef): Unit = {
    val path = ref.path.toStringWithAddress(address)
    val completed = replayState.stopped + path
    val waiting = replayState.running - path
    context.become(pausing(replayState.copy(running = waiting, stopped = completed)))
    if (waiting.isEmpty) {
      self ! DoPreReplay
    }
  }

  def doPreReplay(): Unit = {
    replaySource.preReplay().onComplete {
      case Success(_) ⇒
        self ! PreReplayCompleted
      case Failure(e) ⇒
        log.error(s"An unexpected error happened running replaying $consumerGroup, " +
          s"please try again, if the problem persists, reach out Surge team for support", e)
        self ! ReplayFailed(e)
    }
  }

  def doReplay(replayState: ReplayCoordinator.ReplayState): Unit = {
    val existingPartitions = replayState.assignments.map {
      case (topicPartition, _) ⇒
        topicPartition.partition()
    }
    context.become(replaying(replayState))
    replaySource.replay(consumerGroup, existingPartitions).map { _ ⇒
      ReplayCompleted
    }.recoverWith {
      case err: Throwable ⇒
        log.error("Replay failed", err)
        Future.successful(ReplayFailed(err))
    }.pipeTo(self)(sender)
    ()
  }

  def startStoppedConsumers(state: ReplayState): Unit = {
    state.stopped.foreach { actorPath ⇒
      log.trace(s"Starting stream manager actor with path $actorPath")
      actorSystem.actorSelection(actorPath) ! StartConsuming
    }
  }

  def getTopicAssignments(): Future[Map[TopicPartition, HostPort]] = {
    HostAssignmentTracker.allAssignments.map { assignments ⇒
      assignments.filter {
        case (topicPartition, _) ⇒
          topicPartition.topic() equals topicName
      }
    }.recoverWith {
      case err: Throwable ⇒
        log.error(s"Failed getting all topic consumers for topic $topicName")
        throw err
    }
  }

  def getTopicConsumers(assignments: Map[TopicPartition, HostPort]): Future[List[String]] = {
    val hostPorts = assignments.map { case (_, hostPort) ⇒ hostPort }.toList.distinct
    discoverActors(KafkaStreamManager.serviceIdentifier, hostPorts, List(topicName))
  }
}

