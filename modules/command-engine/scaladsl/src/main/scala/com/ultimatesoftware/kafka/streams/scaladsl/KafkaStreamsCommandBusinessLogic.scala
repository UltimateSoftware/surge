// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.streams.core.{ KafkaStreamsCommandKafkaConfig, SurgeAggregateReadFormatting, SurgeWriteFormatting }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher, NoOpMetricsProvider, NoOpsMetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.AggregateCommandModel

import scala.concurrent.duration._

trait KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event] {

  private val config = ConfigFactory.load()

  def aggregateName: String
  def stateTopic: KafkaTopic
  def eventsTopic: KafkaTopic

  def commandModel: AggregateCommandModel[Agg, Command, Event]

  def readFormatting: SurgeAggregateReadFormatting[Agg]
  def writeFormatting: SurgeWriteFormatting[Agg, Event]

  def commandValidator: AsyncCommandValidator[Command, Agg]

  def aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) ⇒ Boolean = { (_, _, _) ⇒ true }

  // Defaults to noops publishing (for now) and 30 second interval on metrics snapshots
  // These can be overridden in the derived applications
  def metricsPublisher: MetricsPublisher = NoOpsMetricsPublisher
  def metricsInterval: FiniteDuration = 30.seconds

  def metricsProvider: MetricsProvider = NoOpMetricsProvider

  def aggregateIdToString: AggId ⇒ String

  def consumerGroupBase: String = {
    val environment = config.getString("app.environment")
    s"$aggregateName-$environment-command"
  }

  def transactionalIdPrefix: String = "surge-transactional-event-producer-partition"

  private def kafkaConfig: KafkaStreamsCommandKafkaConfig = KafkaStreamsCommandKafkaConfig(stateTopic, eventsTopic)

  private[scaladsl] def toCore: com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[Agg, Command, Event] = {
    new com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[Agg, Command, Event](
      aggregateName = aggregateName, kafka = kafkaConfig,
      model = commandModel, readFormatting = readFormatting,
      writeFormatting = writeFormatting, commandValidator = commandValidator, aggregateValidator = aggregateValidator,
      metricsProvider = metricsProvider, metricsPublisher = metricsPublisher, metricsInterval = metricsInterval,
      consumerGroup = consumerGroupBase,
      transactionalIdPrefix = transactionalIdPrefix)
  }
}
