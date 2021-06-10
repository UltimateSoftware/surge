// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal

import io.opentracing.Tracer
import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.internal.domain.AggregateProcessingModel
import surge.internal.kafka.{ ProducerActorContext, SurgeKafkaConfig }
import surge.kafka.{ KafkaPartitioner, PartitionStringUpToColon }
import surge.metrics.Metrics

trait SurgeModel[S, M, +R, E] extends ProducerActorContext {
  override def aggregateName: String
  def aggregateReadFormatting: SurgeAggregateReadFormatting[S]
  def aggregateWriteFormatting: SurgeAggregateWriteFormatting[S]
  def eventWriteFormattingOpt: Option[SurgeEventWriteFormatting[E]]

  def aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean
  def model: AggregateProcessingModel[S, M, R, E]
  override def metrics: Metrics
  override def tracer: Tracer
  override val kafka: SurgeKafkaConfig
  override val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon
}
