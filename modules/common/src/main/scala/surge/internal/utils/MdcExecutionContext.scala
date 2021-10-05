// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

object MdcExecutionContext {
  implicit lazy val mdcExecutionContext: MdcFuturePropagation = new MdcFuturePropagation(scala.concurrent.ExecutionContext.Implicits.global)
}
