// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import scala.concurrent.ExecutionContext

object MdcExecutionContext {
  implicit lazy val mdcExecutionContext: ExecutionContext = new DiagnosticContextFuturePropagation(scala.concurrent.ExecutionContext.Implicits.global)
}
