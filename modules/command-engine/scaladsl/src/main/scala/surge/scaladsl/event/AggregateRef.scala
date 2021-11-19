// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.internal.scaladsl.event.AggregateRefImpl
import surge.scaladsl.common._

import scala.concurrent.Future

object AggregateRef {
  def apply[AggId, Agg, Event](aggregateId: AggId, region: ActorRef, tracer: Tracer): AggregateRef[Agg, Event] = {
    new AggregateRefImpl(aggregateId, region, tracer)
  }
}

trait AggregateRef[Agg, Event] {
  def getState: Future[Option[Agg]]
  def applyEvent(event: Event): Future[ApplyEventResult[Agg]]
}
