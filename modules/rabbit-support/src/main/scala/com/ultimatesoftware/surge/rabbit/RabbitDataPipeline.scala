// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.surge.rabbit

import com.ultimatesoftware.kafka.streams.core.DataPipeline
import com.ultimatesoftware.kafka.streams.core.DataPipeline.ReplaySuccessfullyStarted

import scala.concurrent.Future

// TODO Refactor StreamManager for Kafka event source to support more generic streams and add a Rabbit StreamManager implementation that
//  we can use to start/stop from here.
private[rabbit] class RabbitDataPipeline extends DataPipeline {
  override def start(): Unit = {}
  override def stop(): Unit = {}
  override def replay(): Future[DataPipeline.ReplayResult] = Future.successful(ReplaySuccessfullyStarted())
}
