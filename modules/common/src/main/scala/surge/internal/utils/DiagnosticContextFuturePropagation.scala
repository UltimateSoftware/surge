// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import io.opentelemetry.api.trace.{ Span, Tracer }
import io.opentelemetry.context.Context
import org.slf4j.MDC

import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor }

/**
 * Execution context proxy for propagating SLF4J diagnostic context and the current span from caller thread to execution thread.
 */
class DiagnosticContextFuturePropagation(executionContext: ExecutionContext) extends ExecutionContext {
  override def execute(runnable: Runnable): Unit = {
    val callerMdc = Option(MDC.getCopyOfContextMap) // get a copy of the MDC context data
    val callerSpan = Span.current()
    println(s"JEFF --- caller MDC = $callerMdc")
    println(s"JEFF --- caller span = $callerSpan")
    executionContext.execute(new Runnable {
      def run(): Unit = {
        callerMdc.foreach(MDC.setContextMap)
        val scope = callerSpan.makeCurrent()
        try {
          println(s"JEFF --- calling run() - scope = $scope, MDC = ${MDC.getCopyOfContextMap}")
          runnable.run()
        } finally {
          // the thread might be reused, so we clean up for the next use
          println("JEFF --- cleaning up future thread")
          MDC.clear()
          scope.close()
        }
      }
    })
  }

  override def reportFailure(cause: Throwable): Unit = executionContext.reportFailure(cause)
}
