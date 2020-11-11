// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import surge.core.DataPipeline
import surge.core.DataPipeline.ReplaySuccessfullyStarted

import scala.concurrent.Future

// TODO Refactor StreamManager for Kafka event source to support more generic streams and add a Rabbit StreamManager implementation that
//  we can use to start/stop from here.
private[rabbit] class RabbitDataPipeline extends DataPipeline {
  override def start(): Unit = {}
  override def stop(): Unit = {}
  override def replay(): Future[DataPipeline.ReplayResult] = Future.successful(ReplaySuccessfullyStarted())
}
