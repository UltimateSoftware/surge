// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import java.util.UUID

import com.typesafe.config.ConfigFactory
import surge.metrics.Metrics
import surge.streams.DataPipeline
import surge.streams.DataPipeline.ReplayResult

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

private[surge] class ManagedDataPipeline(underlyingManager: KafkaStreamManager[_, _], metrics: Metrics) extends DataPipeline {
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
}
