// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import java.util.concurrent.Executors

import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor }

object ThreadPools {
  private val ioBoundThreadPoolSize: Int = 32
  val ioBoundContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(ioBoundThreadPoolSize))
}
