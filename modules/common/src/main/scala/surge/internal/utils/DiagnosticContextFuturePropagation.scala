// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import io.opentelemetry.api.trace.Span
import org.slf4j.MDC

import scala.concurrent.ExecutionContext

/**
 * Execution context proxy for propagating SLF4J diagnostic context and the current span from caller thread to execution thread.
 */
class DiagnosticContextFuturePropagation(executionContext: ExecutionContext) extends ExecutionContext {
  override def execute(runnable: Runnable): Unit = {
    val callerMdc = Option(MDC.getCopyOfContextMap) // get a copy of the MDC context data
    val callerSpan = Span.current()
    executionContext.execute(new Runnable {
      def run(): Unit = {
        callerMdc.foreach(MDC.setContextMap)
        val scope = callerSpan.makeCurrent()
        try {
          runnable.run()
        } finally {
          // the thread might be reused, so we clean up for the next use
          MDC.clear()
          scope.close()
        }
      }
    })
  }

  override def reportFailure(cause: Throwable): Unit = executionContext.reportFailure(cause)
}

object DiagnosticContextFuturePropagation {
  val global = new DiagnosticContextFuturePropagation(ExecutionContext.global)
}
