// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.scaladsl.common._

import scala.concurrent.Future

trait AggregateRef[Agg, Event] {
  def getState: Future[Option[Agg]]
  final def applyEvent(event: Event): Future[ApplyEventResult[Agg]] = applyEvents(List(event))
  def applyEvents(event: Seq[Event]): Future[ApplyEventResult[Agg]]
}

class AggregateRefImpl[AggId, Agg, Event](val aggregateId: AggId, protected val region: ActorRef, protected val tracer: Tracer)
    extends AggregateRef[Agg, Event]
    with AggregateRefBaseTrait[AggId, Agg, Nothing, Event]
