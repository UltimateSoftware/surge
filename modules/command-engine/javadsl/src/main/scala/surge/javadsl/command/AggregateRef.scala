// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.internal.javadsl.command.AggregateRefImpl
import surge.javadsl.common._

import java.util.Optional
import java.util.concurrent.CompletionStage

object AggregateRef {
  def apply[AggId, Agg, Cmd, Event](aggregateId: AggId, actorRef: ActorRef, tracer: Tracer): AggregateRef[Agg, Cmd, Event] = {
    new AggregateRefImpl(aggregateId, actorRef, tracer)
  }
}

trait AggregateRef[Agg, Cmd, Event] {
  def getState: CompletionStage[Optional[Agg]]

  def sendCommand(command: Cmd): CompletionStage[CommandResult[Agg]]

  def applyEvent(event: Event): CompletionStage[ApplyEventResult[Agg]]
}
