// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams.replay

import java.util.concurrent.TimeUnit

import akka.Done
import com.typesafe.config.ConfigFactory
import surge.internal.utils.Logging
import surge.streams.DataHandler

import scala.concurrent.Future
import scala.concurrent.duration.{ FiniteDuration, _ }

trait EventReplayStrategy[Key, Value] {
  def preReplay: () => Future[Any]
  def postReplay: () => Unit
  def replay(consumerGroup: String, partitions: Iterable[Int], replayFlow: DataHandler[Key, Value]): Future[Done]
}

object DefaultEventReplaySettings extends EventReplaySettings {
  private val config = ConfigFactory.load()
  override val entireReplayTimeout: FiniteDuration = config.getDuration("kafka.streams.replay.entire-process-timeout", TimeUnit.MILLISECONDS).milliseconds
}

class NoOpEventReplayStrategy[Key, Value] extends EventReplayStrategy[Key, Value] with Logging {
  override def preReplay: () => Future[Any] = () => Future.successful(true)
  override def postReplay: () => Unit = () => {}
  override def replay(consumerGroup: String, partitions: Iterable[Int], replayFlow: DataHandler[Key, Value]): Future[Done] = {
    log.warn("Event Replay has been used with the default NoOps implementation, please refer to the docs to properly chose your replay strategy")
    Future.successful(Done)
  }
}
