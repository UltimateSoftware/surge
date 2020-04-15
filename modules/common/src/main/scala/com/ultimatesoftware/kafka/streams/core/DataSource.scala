// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.ActorMaterializer
import com.typesafe.config.{ Config, ConfigFactory }
import com.ultimatesoftware.akka.streams.kafka.{ KafkaConsumer, KafkaStreamManager }
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

  def to(sink: DataSink[Key, Value], consumerGroup: String): Unit = {
    val consumerSettings = KafkaConsumer.consumerSettings[Key, Value](actorSystem, groupId = consumerGroup,
      brokers = kafkaBrokers)(keyDeserializer, valueDeserializer)
    to(consumerSettings)(sink)
  }

  private val useNewConsumer = config.getBoolean("surge.use-new-consumer")
  private[core] def to(consumerSettings: ConsumerSettings[Key, Value])(sink: DataSink[Key, Value]): Unit = {
    implicit val system: ActorSystem = actorSystem
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    if (useNewConsumer) {
      new DataPipeline(new KafkaStreamManager(kafkaTopic, consumerSettings, sink.handle, parallelism).start())
    } else {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      KafkaConsumer().streamAndCommitOffsets(kafkaTopic, sink.handle, parallelism, consumerSettings)
    }
  }
}

trait DataSink[Key, Value] {
  def handle(key: Key, value: Value): Future[Any]
}
