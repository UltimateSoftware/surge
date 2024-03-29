// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.tracing

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer

object NoopTracerFactory {

  def create(): Tracer = OpenTelemetry.noop().getTracer(OpenTelemetryInstrumentation.Name, OpenTelemetryInstrumentation.Version)
}
