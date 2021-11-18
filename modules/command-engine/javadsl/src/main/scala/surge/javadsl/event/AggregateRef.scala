// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.internal.javadsl.event.AggregateRefImpl
import surge.javadsl.common._

import java.util.Optional
import java.util.concurrent.CompletionStage

object AggregateRef {
  def apply[AggId, Agg, Event](aggregateId: AggId, actorRef: ActorRef, tracer: Tracer): AggregateRef[Agg, Event] = {
    new AggregateRefImpl(aggregateId, actorRef, tracer)
  }
}

trait AggregateRef[Agg, Event] {
  def getState: CompletionStage[Optional[Agg]]
  def applyEvent(event: Event): CompletionStage[ApplyEventResult[Agg]]
}
