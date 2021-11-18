// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.internal.scaladsl.command.AggregateRefImpl
import surge.scaladsl.common._

import scala.concurrent.Future

object AggregateRef {
  def apply[AggId, Agg, Cmd, Event](aggregateId: AggId, region: ActorRef, tracer: Tracer): AggregateRef[Agg, Cmd, Event] = {
    new AggregateRefImpl(aggregateId, region, tracer)
  }
}

trait AggregateRef[Agg, Cmd, Event] {
  def getState: Future[Option[Agg]]

  def sendCommand(command: Cmd): Future[CommandResult[Agg]]

  def applyEvent(event: Event): Future[ApplyEventResult[Agg]]
}
