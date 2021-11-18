package surge.internal.javadsl.event

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.javadsl.common.AggregateRefBaseTrait
import surge.javadsl.event.AggregateRef

class AggregateRefImpl[AggId, Agg, Event](val aggregateId: AggId, protected val region: ActorRef, protected val tracer: Tracer)
  extends AggregateRef[Agg, Event]
    with AggregateRefBaseTrait[AggId, Agg, Nothing, Event]
