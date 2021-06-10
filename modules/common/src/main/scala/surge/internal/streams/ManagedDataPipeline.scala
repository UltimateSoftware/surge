// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import java.util.UUID

import com.typesafe.config.ConfigFactory
import surge.metrics.Metrics
import surge.streams.DataPipeline
import surge.streams.DataPipeline.ReplayResult
import surge.streams.replay.ReplayControl

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

private[surge] class ManagedDataPipeline[Key, Value](underlyingManager: KafkaStreamManager[Key, Value], metrics: Metrics) extends DataPipeline {
  private val kafkaConsumerMetricsName: String = s"kafka-consumer-metrics-${UUID.randomUUID()}"
  private val config = ConfigFactory.load()
  private val enableMetrics = config.getBoolean("surge.kafka-event-source.enable-kafka-metrics")
  override def stop(): Unit = {
    if (enableMetrics) {
      metrics.unregisterKafkaMetric(kafkaConsumerMetricsName)
    }
    underlyingManager.stop()
  }
  override def start(): Unit = {
    if (enableMetrics) {
      metrics.registerKafkaMetrics(kafkaConsumerMetricsName, () => underlyingManager.getMetricsSynchronous.asJava)
    }
    underlyingManager.start()
  }
  override def replay(): Future[ReplayResult] = {
    underlyingManager.replay()
  }
  override def getReplayControl: ReplayControl = underlyingManager.getReplayControl
}
