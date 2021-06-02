// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global

object Context {
  val noop: Context = new Context()
}

class Context private (private[surge] val executionContext: ExecutionContext = global)
