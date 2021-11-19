package surge.internal.scaladsl.event

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.scaladsl.common.AggregateRefBaseTrait
import surge.scaladsl.event.AggregateRef

class AggregateRefImpl[AggId, Agg, Event](val aggregateId: AggId, protected val region: ActorRef, protected val tracer: Tracer)
    extends AggregateRef[Agg, Event]
    with AggregateRefBaseTrait[AggId, Agg, Nothing, Event]
