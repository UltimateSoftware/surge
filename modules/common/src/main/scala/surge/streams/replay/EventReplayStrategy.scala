// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams.replay

import java.util.concurrent.TimeUnit
import akka.Done
import akka.actor.ActorContext
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import surge.streams.DataHandler

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}

case class ReplayControlContext[Key, Value](keyDeserializer: Array[Byte] => Key,
                                            valueDeserializer: Array[Byte] => Value,
                                            dataHandler: DataHandler[Key, Value])

trait EventReplayStrategy {
  def preReplay: () => Future[Any]
  def replayProgress: ReplayProgress => Unit
  def postReplay: () => Unit
  def createReplayController[Key, Value](context: ReplayControlContext[Key, Value]): ReplayControl
}

trait ReplayLifecycleCallbacks {
  def onResetComplete(resetComplete: ResetComplete): Unit
  def onReplayProgress(replayProgress: ReplayProgress): Unit
}

class NoopReplayLifecycleCallbacks extends ReplayLifecycleCallbacks {
  override def onResetComplete(resetComplete: ResetComplete): Unit = {}

  override def onReplayProgress(replayProgress: ReplayProgress): Unit = {}
}

class ContextForwardingLifecycleCallbacks(context: ActorContext) extends ReplayLifecycleCallbacks {
  override def onResetComplete(resetComplete: ResetComplete): Unit = {
    context.self ! resetComplete
  }

  override def onReplayProgress(replayProgress: ReplayProgress): Unit = {
    context.self ! replayProgress
  }
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
  def fullReplay(consumerGroup: String,
                 partitions: Iterable[Int],
                 replayLifecycleCallbacks: ReplayLifecycleCallbacks = new NoopReplayLifecycleCallbacks()): Future[Done]
  // TODO - Look at Lore integration with gRPC Replay Service
  def getReplayProgress: Future[ReplayProgress]
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
class NoOpEventReplayControl(override val preReplay: () => Future[Any] = () => Future.successful(true),
                             override val postReplay: () => Unit = () => {},
                             override val replayProgress: ReplayProgress => Unit = _ => {}) extends ReplayControl {
  private val log = LoggerFactory.getLogger(getClass)
  override def fullReplay(consumerGroup: String,
                          partitions: Iterable[Int],
                          replayLifecycleCallbacks: ReplayLifecycleCallbacks = new NoopReplayLifecycleCallbacks()): Future[Done] = {
    log.warn("Event Replay has been used with the default NoOps implementation, please refer to the docs to properly chose your replay strategy")
    Future.successful(Done)
  }

  override def getReplayProgress: Future[ReplayProgress] = Future.successful(ReplayProgress())
}
