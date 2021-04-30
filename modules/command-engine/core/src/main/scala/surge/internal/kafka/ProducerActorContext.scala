//
package surge.internal.kafka

import io.opentracing.Tracer
import surge.core.SurgeKafkaConfig
import surge.kafka.{ KafkaPartitioner, PartitionStringUpToColon }
import surge.metrics.Metrics

trait ProducerActorContext {
  def aggregateName: String
  def metrics: Metrics
  def tracer: Tracer
  val kafka: SurgeKafkaConfig
  val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon
}
