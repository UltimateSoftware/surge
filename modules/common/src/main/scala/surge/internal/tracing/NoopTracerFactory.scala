// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.tracing

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer

object NoopTracerFactory {

  def create(): Tracer = OpenTelemetry.noop().getTracer(Instrumentation.Name, Instrumentation.Version)
}
