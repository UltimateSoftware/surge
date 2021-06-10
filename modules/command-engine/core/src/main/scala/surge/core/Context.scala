// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.ActorRef

import scala.concurrent.ExecutionContext

private[surge] object Context {
  def apply(executionContext: ExecutionContext, actorRef: ActorRef): Context = new Context(executionContext, actorRef)

}

private[surge] class Context private (val executionContext: ExecutionContext, val actorRef: ActorRef)
