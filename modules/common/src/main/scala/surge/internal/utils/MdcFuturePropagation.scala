// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import org.slf4j.MDC
import scala.concurrent.ExecutionContext

/**
 * Execution context proxy for propagating SLF4J diagnostic context from caller thread to execution thread.
 */

class MdcFuturePropagation(executionContext: ExecutionContext) extends ExecutionContext {
  override def execute(runnable: Runnable): Unit = {
    val callerMdc = MDC.getCopyOfContextMap // get a copy of the MDC context data
    executionContext.execute(new Runnable {
      def run(): Unit = {
        // copy caller thread MDC context data to the new execution thread
        if (callerMdc != null) MDC.setContextMap(callerMdc)
        try {
          runnable.run()
        } finally {
          // the thread might be reused, so we clean up for the next use
          MDC.clear()
        }
      }
    })
  }

  override def reportFailure(cause: Throwable): Unit = executionContext.reportFailure(cause)
}
