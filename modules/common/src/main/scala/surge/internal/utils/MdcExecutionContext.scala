package surge.internal.utils

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

object MdcExecutionContext {
  implicit lazy val mdcExecutionContext: MdcFuturePropagation = new MdcFuturePropagation(
    ExecutionContext.fromExecutor( // this gives us an ExecutionContext we piggyback on in MdcExecutionContext
      Executors.newWorkStealingPool(10) // this creates the ForkJoinPool with a maximum of 10 threads
    ))
}
