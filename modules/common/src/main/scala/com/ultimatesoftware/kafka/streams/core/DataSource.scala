// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.scaladsl.Flow
import com.typesafe.config.{ Config, ConfigFactory }
import com.ultimatesoftware.akka.streams.kafka.{ KafkaConsumer, KafkaStreamManager }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

trait DataSource {
  def replayStrategy: EventReplayStrategy = NoOpEventReplayStrategy
  def replaySettings: EventReplaySettings = DefaultEventReplaySettings
}

trait KafkaDataSource[Key, Value] extends DataSource {
  private val config: Config = ConfigFactory.load()
  private val defaultBrokers = config.getString("kafka.brokers")

  def kafkaBrokers: String = defaultBrokers
  def kafkaTopic: KafkaTopic

  def actorSystem: ActorSystem

  def keyDeserializer: Deserializer[Key]
  def valueDeserializer: Deserializer[Value]

  def to(sink: DataHandler[Key, Value], consumerGroup: String): DataPipeline = {
    val consumerSettings = KafkaConsumer.consumerSettings[Key, Value](actorSystem, groupId = consumerGroup,
      brokers = kafkaBrokers)(keyDeserializer, valueDeserializer)
    to(consumerSettings)(sink)
  }

  private[core] def to(consumerSettings: ConsumerSettings[Key, Value])(sink: DataHandler[Key, Value]): DataPipeline = {
    implicit val system: ActorSystem = actorSystem
    implicit val executionContext: ExecutionContext = ExecutionContext.global

    new ManagedDataPipelineImpl(KafkaStreamManager(kafkaTopic, consumerSettings, replayStrategy, replaySettings, sink.dataHandler).start())
  }
}

object FlowConverter {
  private val log = LoggerFactory.getLogger(getClass)

  def flowFor[T](
    businessLogic: T ⇒ Future[Any],
    parallelism: Int)(implicit ec: ExecutionContext): Flow[T, Done, NotUsed] = {
    Flow[T].mapAsync(parallelism) { t ⇒
      businessLogic(t).recover {
        case e ⇒
          log.error(s"An exception was thrown by the event handler! The stream will restart and the message will be retried.", e)
          throw e
      }.map(_ ⇒ Done)
    }
  }
}

trait DataHandler[Key, Value] {
  def dataHandler: Flow[(Key, Value), Any, NotUsed]
}
trait DataSink[Key, Value] extends DataHandler[Key, Value] {
  def parallelism: Int = 1
  def handle(key: Key, value: Value): Future[Any]

  override def dataHandler: Flow[(Key, Value), Any, NotUsed] = {
    val tupHandler: ((Key, Value)) ⇒ Future[Any] = { tup ⇒ handle(tup._1, tup._2) }
    FlowConverter.flowFor(tupHandler, parallelism)(ExecutionContext.global)
  }
}
