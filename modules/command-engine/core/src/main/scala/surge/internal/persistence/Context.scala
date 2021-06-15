// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import scala.concurrent.ExecutionContext

private[surge] object Context {
  def apply(executionContext: ExecutionContext, actor: PersistentActor[_, _, _, _]): Context = new Context(executionContext, actor)
}

private[surge] class Context private (val executionContext: ExecutionContext, val actor: PersistentActor[_, _, _, _])
