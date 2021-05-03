// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>
package surge.core

import io.opentracing.Tracer
import surge.internal.kafka.ProducerActorContext
import surge.kafka.{ KafkaPartitioner, PartitionStringUpToColon }
import surge.metrics.Metrics

trait SurgeModel[S, E] extends ProducerActorContext {
  override def aggregateName: String
  def readFormatting: SurgeAggregateReadFormatting[S]
  def writeFormatting: SurgeWriteFormatting[S, E]
  def aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean
  override def metrics: Metrics
  override def tracer: Tracer
  override val kafka: SurgeCommandKafkaConfig
  override val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon
}
