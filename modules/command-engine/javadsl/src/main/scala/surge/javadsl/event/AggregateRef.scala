// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.javadsl.common._

import java.util.Optional
import java.util.concurrent.CompletionStage

trait AggregateRef[Agg, Event, Response] {
  def getState: CompletionStage[Optional[Agg]]
  def applyEvent(event: Event): CompletionStage[ApplyEventResult[Response]]
}

class AggregateRefImpl[AggId, Agg, Event, Response](val aggregateId: AggId, protected val region: ActorRef, protected val tracer: Tracer)
    extends AggregateRef[Agg, Event, Response]
    with AggregateRefBaseTrait[AggId, Agg, Nothing, Event, Response]
