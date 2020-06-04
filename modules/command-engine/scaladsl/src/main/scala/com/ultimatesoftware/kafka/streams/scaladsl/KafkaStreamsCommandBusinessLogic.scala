// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.streams.core.{ KafkaStreamsCommandKafkaConfig, SurgeAggregateReadFormatting, SurgeWriteFormatting }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher, NoOpMetricsProvider, NoOpsMetricsPublisher }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.AggregateCommandModel

import scala.concurrent.duration._

trait KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta] {

  private val config = ConfigFactory.load()

  def aggregateName: String
  def stateTopic: KafkaTopic
  def eventsTopic: KafkaTopic
  @deprecated("Metadata topic is no longer used", "0.4.29")
  def internalMetadataTopic: KafkaTopic = KafkaTopic("")
  def eventKeyExtractor: Event ⇒ String

  def commandModel: AggregateCommandModel[AggId, Agg, Command, Event, CmdMeta, EvtMeta]

  def readFormatting: SurgeAggregateReadFormatting[AggId, Agg]
  def writeFormatting: SurgeWriteFormatting[AggId, Agg, Event, EvtMeta]

  def commandValidator: AsyncCommandValidator[Command, Agg]

  def aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) ⇒ Boolean = { (_, _, _) ⇒ true }

  // Defaults to noops publishing (for now) and 30 second interval on metrics snapshots
  // These can be overridden in the derived applications
  def metricsPublisher: MetricsPublisher = NoOpsMetricsPublisher
  def metricsInterval: FiniteDuration = 30.seconds

  def metricsProvider: MetricsProvider = NoOpMetricsProvider

  def consumerGroup: String = {
    val environment = config.getString("app.environment")
    s"$aggregateName-$environment-command"
  }

  def transactionalIdPrefix: String = "surge-transactional-event-producer-partition"

  private def kafkaConfig: KafkaStreamsCommandKafkaConfig[Event] = KafkaStreamsCommandKafkaConfig(stateTopic, eventsTopic, eventKeyExtractor)

  private[scaladsl] def toCore: com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta] = {
    new com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
      aggregateName = aggregateName, kafka = kafkaConfig,
      model = commandModel, readFormatting = readFormatting,
      writeFormatting = writeFormatting, commandValidator = commandValidator, aggregateValidator = aggregateValidator,
      metricsProvider = metricsProvider, metricsPublisher = metricsPublisher, metricsInterval = metricsInterval,
      consumerGroup = consumerGroup,
      transactionalIdPrefix = transactionalIdPrefix)
  }
}
