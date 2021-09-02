// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import akka.actor.{ Actor, ActorRef, Address }
import akka.pattern.pipe
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import surge.exceptions.SurgeReplayException
import surge.internal.akka.cluster.{ ActorHostAwareness, ActorRegistry }
import surge.internal.kafka.HostAssignmentTracker
import surge.internal.streams.KafkaStreamManagerActor.{ StartConsuming, SuccessfullyStopped }
import surge.internal.utils.InlineReceive
import surge.kafka.HostPort
import surge.streams.replay.{ ContextForwardingLifecycleCallbacks => ReplayCoordinationCallbacks, ReplayComplete, ReplayControl, ReplayProgress, ReplayReady }

import scala.concurrent.Future
import scala.util.{ Failure, Success }

private[streams] object ReplayCoordinator {
  sealed trait ReplayCoordinatorRequest
  case object StartReplay extends ReplayCoordinatorRequest
  case object StopReplay extends ReplayCoordinatorRequest
  case object DoPreReplay extends ReplayCoordinatorRequest
  case object GetReplayProgress extends ReplayCoordinatorRequest

  sealed trait ReplayCoordinatorResponse
  case object ReplayStarted extends ReplayCoordinatorResponse
  case object ReplayCompleted extends ReplayCoordinatorResponse
  case class ReplayFailed(reason: Throwable) extends ReplayCoordinatorResponse

  case class ReplayState(replyTo: ActorRef, running: Set[String], stopped: Set[String], assignments: Map[TopicPartition, HostPort], progress: ReplayProgress)
  object ReplayState {
    def init(sender: ActorRef): ReplayState = ReplayState(sender, Set(), Set(), Map(), ReplayProgress())
  }
}

class ReplayCoordinator(topicName: String, consumerGroup: String, registry: ActorRegistry, replayControl: ReplayControl) extends Actor with ActorHostAwareness {

  import ReplayCoordinator._
  import context.dispatcher

  private val log = LoggerFactory.getLogger(getClass)

  private sealed trait Internal
  private case class TopicConsumersFound(assignments: Map[TopicPartition, HostPort], actorPaths: List[String])
  private case class TopicAssignmentsFound(assignments: Map[TopicPartition, HostPort])
  private case object PreReplayCompleted

  override def receive: Receive = uninitialized()

  private def uninitialized(): Receive = { case StartReplay =>
    context.become(ready(ReplayState.init(sender())))
    getTopicAssignments.map(assignments => TopicAssignmentsFound(assignments)).pipeTo(self)
  }

  private def ready(replayState: ReplayState): Receive = InlineReceive {
    case TopicAssignmentsFound(assignments) =>
      getTopicConsumers(assignments).map(actors => TopicConsumersFound(assignments, actors)).pipeTo(self)
    case msg: TopicConsumersFound => handleTopicConsumersFound(replayState, msg)
  }.orElse(handleStopReplay(replayState))

  private def pausing(replayState: ReplayState): Receive = InlineReceive {
    case SuccessfullyStopped(address, ref) =>
      handleSuccessfullyStopped(replayState, address, ref)
    case PreReplayCompleted =>
      doReplay(replayState)
    case DoPreReplay =>
      doPreReplay()
  }.orElse(handleStopReplay(replayState))

  private def replaying(replayState: ReplayState): Receive = InlineReceive {
    case failure: ReplayFailed =>
      startStoppedConsumers(replayState)
      replayState.replyTo ! failure
      context.become(uninitialized())
    case ReplayStarted =>
      log.debug("Replay Started")
      replayState.replyTo ! ReplayStarted
    case progress: ReplayProgress =>
      replayControl.replayProgress(progress)
      if (progress.isComplete) {
        context.self ! ReplayCompleted
      }
      // update progress in state
      context.become(replaying(replayState.copy(progress = progress)))
    case _: ReplayReady =>
      startStoppedConsumers(replayState)
    case ReplayComplete() =>
      context.self ! ReplayCompleted
    case ReplayCompleted =>
      replayControl.postReplay()
      replayState.replyTo ! ReplayCompleted
      context.become(uninitialized())
    case GetReplayProgress =>
      sender() ! replayState.progress
  }.orElse(handleStopReplay(replayState))

  private def handleStopReplay(replayState: ReplayState): Receive = { case ReplayCoordinator.StopReplay =>
    log.debug("StopReplay received, this is typically because a timeout of the ReplayCoordinator or an unexpected error")
    startStoppedConsumers(replayState)
    context.become(uninitialized())
  }

  private def handleSuccessfullyStopped(replayState: ReplayState, address: Address, ref: ActorRef): Unit = {
    val path = ref.path.toStringWithAddress(address)
    val completed = replayState.stopped + path
    val waiting = replayState.running - path
    context.become(pausing(replayState.copy(running = waiting, stopped = completed)))
    log.debug("ReplayCoordinator saw a StreamManager at [{}] stop, waiting for [{}] more managers to stop before proceeding", path, waiting.size)
    if (waiting.isEmpty) {
      self ! DoPreReplay
    }
  }

  private def doPreReplay(): Unit = {
    log.trace("ReplayCoordinator kicking off PreReplay function")
    replayControl.preReplay().onComplete {
      case Success(_) =>
        self ! PreReplayCompleted
      case Failure(e) =>
        log.error(
          s"An unexpected error happened running replaying $consumerGroup, " +
            s"please try again, if the problem persists, reach out Surge team for support",
          e)
        self ! ReplayFailed(e)
    }
  }

  private def doReplay(replayState: ReplayCoordinator.ReplayState): Unit = {
    log.trace("ReplayCoordinator kicking off replay")
    val existingPartitions = replayState.assignments.map { case (topicPartition, _) =>
      topicPartition.partition()
    }
    context.become(replaying(replayState))
    replayControl
      .fullReplay(consumerGroup, existingPartitions, new ReplayCoordinationCallbacks(context))
      .map { _ =>
        ReplayStarted
      }
      .recover { case err: Throwable =>
        log.error("Replay failed", err)
        ReplayFailed(err)
      }
      .pipeTo(self)(sender())
  }

  private def startStoppedConsumers(state: ReplayState): Unit = {
    state.stopped.foreach { actorPath =>
      log.trace(s"Starting stream manager actor with path $actorPath")
      actorSystem.actorSelection(actorPath) ! StartConsuming
    }
  }

  private def handleTopicConsumersFound(replayState: ReplayState, topicConsumersFound: TopicConsumersFound): Unit = {
    val topicConsumerActors = topicConsumersFound.actorPaths
    val topicAssignments = topicConsumersFound.assignments
    val waitingActorsPaths = topicConsumerActors.toSet
    context.become(pausing(replayState.copy(running = waitingActorsPaths, stopped = Set.empty, assignments = topicAssignments)))
    if (topicConsumerActors.isEmpty) {
      log.warn("Could not find any registered StreamManagers for topic [{}]. Bailing out of replay...", topicName)
      replayState.replyTo ! ReplayFailed(new SurgeReplayException(s"Could not find any registered StreamManagers for topic $topicName"))
      self ! StopReplay
    }
    topicConsumerActors.foreach { streamManagerActorPath =>
      actorSystem.actorSelection(streamManagerActorPath) ! KafkaStreamManagerActor.StopConsuming
    }
  }

  private def getTopicAssignments: Future[Map[TopicPartition, HostPort]] = {
    HostAssignmentTracker.allAssignments
      .map { assignments =>
        assignments.filter { case (topicPartition, _) =>
          topicPartition.topic().equals(topicName)
        }
      }
      .recoverWith { case err: Throwable =>
        log.error(s"Failed getting all topic consumers for topic $topicName", err)
        throw err
      }
  }

  private def getTopicConsumers(assignments: Map[TopicPartition, HostPort]): Future[List[String]] = {
    val hostPorts = assignments.map { case (_, hostPort) => hostPort }.toList.distinct
    registry.discoverActors(KafkaStreamManager.serviceIdentifier, hostPorts, List(topicName))
  }
}
