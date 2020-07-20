// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._
import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import com.typesafe.config.{ Config, ConfigFactory }
import com.ultimatesoftware.akka.streams.kafka.{ KafkaConsumer, KafkaStreamManager }
import com.ultimatesoftware.kafka.streams.core.DataPipeline._
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import org.apache.kafka.common.serialization.Deserializer

import scala.concurrent.{ ExecutionContext, Future }

trait DataSource[Key, Value] {
  private val config: Config = ConfigFactory.load()
  private val defaultBrokers = config.getString("kafka.brokers")

  def kafkaBrokers: String = defaultBrokers
  def kafkaTopic: KafkaTopic
  def parallelism: Int

  def actorSystem: ActorSystem

  def keyDeserializer: Deserializer[Key]
  def valueDeserializer: Deserializer[Value]
  def replayStrategy: EventReplayStrategy = NoOpEventReplayStrategy
  def replaySettings: EventReplaySettings = DefaultEventReplaySettings

  def to(sink: DataSink[Key, Value], consumerGroup: String): DataPipeline = {
    val consumerSettings = KafkaConsumer.consumerSettings[Key, Value](actorSystem, groupId = consumerGroup,
      brokers = kafkaBrokers)(keyDeserializer, valueDeserializer)
    to(consumerSettings)(sink)
  }

  private val useNewConsumer = config.getBoolean("surge.use-new-consumer")
  private[core] def to(consumerSettings: ConsumerSettings[Key, Value])(sink: DataSink[Key, Value]): DataPipeline = {
    implicit val system: ActorSystem = actorSystem
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    if (useNewConsumer) {
      new ManagedDataPipelineImpl(KafkaStreamManager(kafkaTopic, consumerSettings, replayStrategy, replaySettings, sink.handle, parallelism).start())
    } else {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      KafkaConsumer().streamAndCommitOffsets(kafkaTopic, sink.handle, parallelism, consumerSettings)
      NoOpDataPipelineImpl
    }
  }
}

trait DataSink[Key, Value] {
  def handle(key: Key, value: Value): Future[Any]
}

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
