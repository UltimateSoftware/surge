// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import java.util.UUID
import java.util.concurrent.CompletionStage

import com.typesafe.config.ConfigFactory
import surge.akka.streams.kafka.KafkaStreamManager
import surge.core.DataPipeline.{ ReplayResult, ReplaySuccessfullyStarted }
import surge.metrics.Metrics

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

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

private[core] class ManagedDataPipelineImpl(underlyingManager: KafkaStreamManager[_, _], metrics: Metrics) extends DataPipeline {
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

private[core] object NoOpDataPipelineImpl extends DataPipeline {
  override def start(): Unit = {}
  override def stop(): Unit = {}
  override def replay(): Future[ReplayResult] = Future.successful(ReplaySuccessfullyStarted())
}
