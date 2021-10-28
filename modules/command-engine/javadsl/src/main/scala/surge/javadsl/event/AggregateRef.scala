// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.javadsl.common._

import java.util.Optional
import java.util.concurrent.CompletionStage

trait AggregateRef[Agg, Event] {
  def getState: CompletionStage[Optional[Agg]]
  final def applyEvent(event: Event): CompletionStage[ApplyEventResult[Agg]] = applyEvents(List(event))
  def applyEvents(event: Seq[Event]): CompletionStage[ApplyEventResult[Agg]]
}

class AggregateRefImpl[AggId, Agg, Event](val aggregateId: AggId, protected val region: ActorRef, protected val tracer: Tracer)
    extends AggregateRef[Agg, Event]
    with AggregateRefBaseTrait[AggId, Agg, Nothing, Event]
