// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.scaladsl.common._

import scala.concurrent.Future

trait AggregateRef[Agg, Event, Response] {
  def getState: Future[Option[Agg]]
  def applyEvent(event: Event): Future[ApplyEventResult[Response]]
}

class AggregateRefImpl[AggId, Agg, Event, Response](val aggregateId: AggId, protected val region: ActorRef, protected val tracer: Tracer)
    extends AggregateRef[Agg, Event, Response]
    with AggregateRefBaseTrait[AggId, Agg, Nothing, Event, Response]
