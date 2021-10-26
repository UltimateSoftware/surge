// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import surge.internal.utils.DiagnosticContextFuturePropagation

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

object ThreadPools {
  private val ioBoundThreadPoolSize: Int = 32
  val ioBoundContext: ExecutionContext = new DiagnosticContextFuturePropagation(
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(ioBoundThreadPoolSize)))
}
