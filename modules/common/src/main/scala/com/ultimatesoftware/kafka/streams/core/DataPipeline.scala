// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.util.concurrent.CompletionStage

import com.ultimatesoftware.akka.streams.kafka.KafkaStreamManager
import com.ultimatesoftware.kafka.streams.core.DataPipeline.{ ReplayResult, ReplaySuccessfullyStarted }

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

trait DataPipeline {
  def start(): Unit
  def stop(): Unit
  def replay(): Future[ReplayResult]
  def replayWithCompletionStage(): CompletionStage[ReplayResult] = {
    replay().toJava
  }
}

object DataPipeline {
  sealed trait ReplayResult
  // This is a case class on purpose, Kotlin doesn't do pattern matching against scala case objects :(
  case class ReplaySuccessfullyStarted() extends ReplayResult
  case class ReplayFailed(reason: Throwable) extends ReplayResult
}

class TypedDataPipeline[Type](dataPipeline: DataPipeline) extends DataPipeline {
  override def start(): Unit = dataPipeline.start()
  override def stop(): Unit = dataPipeline.stop()
  override def replay(): Future[ReplayResult] = dataPipeline.replay()
}

private[core] class ManagedDataPipelineImpl(underlyingManager: KafkaStreamManager[_, _]) extends DataPipeline {
  override def stop(): Unit = {
    underlyingManager.stop()
  }
  override def start(): Unit = {
    underlyingManager.start()
  }
  override def replay(): Future[ReplayResult] = {
    underlyingManager.replay()
  }
}

private[core] object NoOpDataPipelineImpl extends DataPipeline {
  override def start(): Unit = {}
  override def stop(): Unit = {}
  override def replay(): Future[ReplayResult] = Future.successful(ReplaySuccessfullyStarted())
}
