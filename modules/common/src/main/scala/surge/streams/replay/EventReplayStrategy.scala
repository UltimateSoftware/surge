// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams.replay

import java.util.concurrent.TimeUnit
import akka.Done
import akka.actor.ActorContext
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import surge.streams.DataHandler

import scala.concurrent.Future
import scala.concurrent.duration.{ FiniteDuration, _ }

trait ReplayCoordinatorApi {
  def resumeConsumers(): Unit
  def getReplayProgress: Future[ReplayProgress]
}

case class ReplayControlContext[Key, Value](keyDeserializer: Array[Byte] => Key, valueDeserializer: Array[Byte] => Value, dataHandler: DataHandler[Key, Value])

trait EventReplayStrategy {
  def preReplay: () => Future[Any]
  def replayProgress: ReplayProgress => Unit
  def postReplay: () => Unit
  def createReplayController[Key, Value](context: ReplayControlContext[Key, Value]): ReplayControl
}

class NoopReplayLifecycleCallbacks extends ReplayLifecycleCallbacks {
  override def onReplayStarted(replayStarted: ReplayStarted): Unit = {}
  override def onReplayReadyForMonitoring(replayReady: ReplayReadyForMonitoring): Unit = {}
  override def onReplayProgress(replayProgress: ReplayProgress): Unit = {}

  override def onReplayComplete(replayComplete: ReplayComplete): Unit = {}

  override def onReplayFailed(replayFailed: ReplayFailed): Unit = {}
}

class ContextForwardingLifecycleCallbacks(context: ActorContext) extends ReplayLifecycleCallbacks {
  override def onReplayStarted(replayStarted: ReplayStarted): Unit = {
    context.self ! replayStarted
  }

  override def onReplayReadyForMonitoring(replayReady: ReplayReadyForMonitoring): Unit = {
    context.self ! replayReady
  }

  override def onReplayProgress(replayProgress: ReplayProgress): Unit = {
    context.self ! replayProgress
  }

  override def onReplayComplete(replayComplete: ReplayComplete): Unit = {
    context.self ! replayComplete
  }

  override def onReplayFailed(replayFailed: ReplayFailed): Unit = {
    context.self ! replayFailed
  }
}

trait ReplayProgressMonitor {
  def getReplayProgress: Future[ReplayProgress]

  def stop(): Unit
}

/**
 * The ReplayControl is a low level control object for performing replay. By default it has a preReplay, postReplay, and fullReplay function. The ReplayControl
 * itself does not coordinate stopping any currently running consumers - it simply provides a way to access various replay functionality for callers who need
 * replay-like functionality outside of a typical full/coordinated replay.
 */
trait ReplayControl {
  def preReplay: () => Future[Any]
  def postReplay: () => Unit

  def replayProgress: ReplayProgress => Unit
  def monitorProgress(coordinatorApi: ReplayCoordinatorApi): ReplayProgressMonitor

  def computeProgress(current: Map[TopicPartition, OffsetAndMetadata], end: Map[TopicPartition, OffsetAndMetadata]): ReplayProgress = {
    val sumCurrent = sum(current)
    val sumEnd = sum(end)

    if (sumEnd > 0) {
      val percentComplete = (sumCurrent / sumEnd) * 100.0
      ReplayProgress(percentComplete)
    } else {
      ReplayProgress.start()
    }
  }

  final def fullReplay(consumerGroup: String, partitions: Iterable[Int]): Future[Done] = {
    fullReplay(consumerGroup, partitions, new NoopReplayLifecycleCallbacks())
  }

  def fullReplay(
      consumerGroup: String,
      partitions: Iterable[Int],
      replayLifecycleCallbacks: ReplayLifecycleCallbacks = new NoopReplayLifecycleCallbacks()): Future[Done]

  private def sum(offsets: Map[TopicPartition, OffsetAndMetadata]): Long = {
    offsets.values.map[Long](o => Option(o).map[Long](offsetPlusMeta => offsetPlusMeta.offset()).getOrElse(0L)).sum
  }
}

object DefaultEventReplaySettings extends EventReplaySettings {
  private val config = ConfigFactory.load()
  override val entireReplayTimeout: FiniteDuration = config.getDuration("kafka.streams.replay.entire-process-timeout", TimeUnit.MILLISECONDS).milliseconds
}

class NoOpEventReplayStrategy extends EventReplayStrategy {
  override def preReplay: () => Future[Any] = () => Future.successful(true)
  override def postReplay: () => Unit = () => {}
  override def replayProgress: ReplayProgress => Unit = _ => {}

  override def createReplayController[Key, Value](context: ReplayControlContext[Key, Value]): NoOpEventReplayControl =
    new NoOpEventReplayControl(preReplay, postReplay, replayProgress)

}

class NoOpReplayProgressMonitor extends ReplayProgressMonitor {
  override def getReplayProgress: Future[ReplayProgress] = Future.successful(ReplayProgress.complete())

  override def stop(): Unit = {}
}

class NoOpEventReplayControl(
    override val preReplay: () => Future[Any] = () => Future.successful(true),
    override val postReplay: () => Unit = () => {},
    override val replayProgress: ReplayProgress => Unit = _ => {})
    extends ReplayControl {
  private val log = LoggerFactory.getLogger(getClass)

  override def fullReplay(
      consumerGroup: String,
      partitions: Iterable[Int],
      replayLifecycleCallbacks: ReplayLifecycleCallbacks = new NoopReplayLifecycleCallbacks()): Future[Done] = {
    log.warn("Event Replay has been used with the default NoOps implementation, please refer to the docs to properly chose your replay strategy")
    Future.successful(Done)
  }

  override def monitorProgress(coordinatorApi: ReplayCoordinatorApi): ReplayProgressMonitor = {
    new NoOpReplayProgressMonitor()
  }
}
