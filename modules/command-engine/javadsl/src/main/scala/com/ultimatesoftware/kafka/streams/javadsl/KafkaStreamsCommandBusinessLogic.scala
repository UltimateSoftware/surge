// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional

import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.streams.core.{ KafkaStreamsCommandKafkaConfig, SurgeAggregateReadFormatting, SurgeWriteFormatting }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher, NoOpMetricsProvider, NoOpsMetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.AggregateCommandModel

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._

abstract class KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event] {

  private val config = ConfigFactory.load()

  def aggregateName: String
  def stateTopic: KafkaTopic
  def eventsTopic: KafkaTopic

  def commandModel: AggregateCommandModel[Agg, Command, Event]

  def readFormatting: SurgeAggregateReadFormatting[Agg]
  def writeFormatting: SurgeWriteFormatting[Agg, Event]

  def commandValidator: AsyncCommandValidator[Command, Agg]

  def aggregateIdToString(aggId: AggId): String = aggId.toString

  def aggregateValidator(key: String, aggJson: Array[Byte], prevAggJsonOpt: Optional[Array[Byte]]): Boolean = true
  private def scalaAggregateValidator: (String, Array[Byte], Option[Array[Byte]]) ⇒ Boolean = { (key, agg, prevAgg) ⇒
    aggregateValidator(key, agg, prevAgg.asJava)
  }

  // Defaults to noops publishing (for now) and 30 second interval on metrics snapshots
  // These can be overridden in the derived applications
  def metricsPublisher: MetricsPublisher = NoOpsMetricsPublisher
  def metricsInterval: FiniteDuration = 30.seconds

  def metricsProvider: MetricsProvider = NoOpMetricsProvider

  private def kafkaConfig = KafkaStreamsCommandKafkaConfig(stateTopic = stateTopic, eventsTopic = eventsTopic)

  def consumerGroupBase: String = {
    val environment = config.getString("app.environment")
    s"$aggregateName-$environment-command"
  }

  def transactionalIdPrefix: String = "surge-transactional-event-producer-partition"

  private[javadsl] def toCore: com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[Agg, Command, Event] = {
    new com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[Agg, Command, Event](
      aggregateName = aggregateName, kafka = kafkaConfig,
      model = commandModel, writeFormatting = writeFormatting, readFormatting = readFormatting,
      commandValidator = commandValidator, aggregateValidator = scalaAggregateValidator,
      metricsProvider = metricsProvider, metricsPublisher = metricsPublisher, metricsInterval = metricsInterval,
      consumerGroup = consumerGroupBase,
      transactionalIdPrefix = transactionalIdPrefix)
  }
}
