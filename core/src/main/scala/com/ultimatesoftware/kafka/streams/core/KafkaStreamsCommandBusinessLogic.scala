// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.scala.core.domain._
import com.ultimatesoftware.scala.core.kafka.{ KafkaPartitioner, KafkaTopic, PartitionStringUpToColon }
import com.ultimatesoftware.scala.core.monitoring.metrics.MetricsPublisher

import scala.concurrent.duration._

private[streams] case class KafkaStreamsCommandBusinessLogic[Agg, AggIdType, Command, Event, CmdMeta](
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    internalMetadataTopic: KafkaTopic,
    businessLogicAdapter: DomainBusinessLogicAdapter[Agg, AggIdType, Command, Event, _, CmdMeta],
    metricsPublisher: MetricsPublisher, metricsInterval: FiniteDuration) {
  val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon
}
