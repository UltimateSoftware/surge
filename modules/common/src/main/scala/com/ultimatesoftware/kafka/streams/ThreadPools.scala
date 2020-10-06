// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.util.concurrent.Executors

import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor }

object ThreadPools {
  private val ioBoundThreadPoolSize: Int = 32
  val ioBoundContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(ioBoundThreadPoolSize))
}
