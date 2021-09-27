package surge.internal.utils

object MdcExecutionContext {
  implicit lazy val mdcExecutionContext: MdcFuturePropagation = new MdcFuturePropagation(scala.concurrent.ExecutionContext.Implicits.global)
}
