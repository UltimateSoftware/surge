// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import io.opentelemetry.api.trace.Tracer
import surge.kafka.{ KafkaPartitioner, PartitionStringUpToColon }
import surge.metrics.Metrics

trait ProducerActorContext {
  def aggregateName: String
  def metrics: Metrics
  def tracer: Tracer
  val kafka: SurgeKafkaConfig
  val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon
}
