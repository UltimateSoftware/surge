// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.util.concurrent.TimeUnit

import akka.Done
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.support.Logging
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait EventReplayStrategy {
  def preReplay: () ⇒ Future[Any]
  def postReplay: () ⇒ Unit
  def replay(consumerGroup: String, partitions: Iterable[Int]): Future[Done]
}

object DefaultEventReplaySettings extends EventReplaySettings {
  val config = ConfigFactory.load()
  val entireProcessTimeout: FiniteDuration = config.getDuration("kafka.streams.replay.entire-process-timeout", TimeUnit.MILLISECONDS).milliseconds
  override val entireReplayTimeout: FiniteDuration = entireProcessTimeout
}

case object NoOpEventReplayStrategy extends EventReplayStrategy with Logging {
  override def preReplay: () ⇒ Future[Any] = () ⇒ Future.successful(true)
  override def postReplay: () ⇒ Unit = () ⇒ {}
  override def replay(
    consumerGroup: String,
    partitions: Iterable[Int]): Future[Done] = {
    log.warn("Event Replay has been used with the default NoOps implementation, please refer to the docs to properly chose your replay strategy")
    Future.successful(Done)
  }
}
