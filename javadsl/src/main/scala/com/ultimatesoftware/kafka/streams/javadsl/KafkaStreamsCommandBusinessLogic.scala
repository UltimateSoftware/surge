// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.javadsl

import com.ultimatesoftware.scala.core.domain._
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher, NoOpsMetricsPublisher }

import scala.concurrent.duration._

abstract class KafkaStreamsCommandBusinessLogic[Agg, AggIdType, Command, Event, Resource, CmdMeta] {

  def stateTopic: KafkaTopic
  def eventsTopic: KafkaTopic

  def internalMetadataTopic: KafkaTopic

  def domainBusinessLogicAdapter: DomainBusinessLogicAdapter[Agg, AggIdType, Command, Event, Resource, CmdMeta]

  // Defaults to noops publishing (for now) and 30 second interval on metrics snapshots
  // These can be overridden in the derived applications
  def metricsPublisher: MetricsPublisher = NoOpsMetricsPublisher
  def metricsInterval: FiniteDuration = 30.seconds

  def metricsProvider: MetricsProvider

  private[javadsl] def toCore: com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[Agg, AggIdType, Command, Event, Resource, CmdMeta] = {
    new com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic[Agg, AggIdType, Command, Event, Resource, CmdMeta](
      stateTopic = stateTopic, eventsTopic = eventsTopic, internalMetadataTopic = internalMetadataTopic,
      businessLogicAdapter = domainBusinessLogicAdapter,
      metricsPublisher = metricsPublisher, metricsInterval = metricsInterval)
  }
}
