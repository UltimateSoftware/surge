// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import surge.internal.tracing.OpenTelemetryInstrumentation
import surge.kafka.{ KafkaPartitioner, PartitionStringUpToColon }
import surge.metrics.Metrics

trait ProducerActorContext {
  def aggregateName: String
  def metrics: Metrics
  val openTelemetry: OpenTelemetry
  def tracer: Tracer = openTelemetry.getTracer(OpenTelemetryInstrumentation.Version, OpenTelemetryInstrumentation.Name)
  val kafka: SurgeKafkaConfig
  val partitioner: KafkaPartitioner[String] = PartitionStringUpToColon.instance
}
