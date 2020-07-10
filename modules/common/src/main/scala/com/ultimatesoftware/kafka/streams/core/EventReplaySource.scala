// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.util.concurrent.TimeUnit

import akka.Done
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.streams.core.KafkaForeverReplaySourceSettings.config
import com.ultimatesoftware.support.Logging

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait EventReplaySource[Event, EvtMeta] {
  def preReplay: () ⇒ Boolean
  def postReplay: () ⇒ Unit
  def replay(consumerGroup: String, partitions: Iterable[Int]): Future[Done]
}

object DefaultEventSourceSettings extends ReplaySourceSettings {
  val config = ConfigFactory.load()
  val entireProcessTimeout: FiniteDuration = config.getDuration("kafka.streams.replay.entire-process-timeout", TimeUnit.MILLISECONDS).milliseconds
  override val entireReplayTimeout: FiniteDuration = entireProcessTimeout
}
object EventReplaySource extends Logging {
  def noOps[Key, Value](): EventReplaySource[Key, Value] = {
    new EventReplaySource[Key, Value] {
      override def preReplay: () ⇒ Boolean = () ⇒ true
      override def postReplay: () ⇒ Unit = () ⇒ {}
      override def replay(
        consumerGroup: String,
        partitions: Iterable[Int]): Future[Done] = {
        log.warn("Event Replay has been used with the default NoOps implementation, please refer to the docs to properly chose your replay strategy")
        Future.successful(Done)
      }
    }
  }
}
