// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.common

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRefBaseTrait[AggId, Agg, Cmd, Event, Response] extends AggregateRefTrait[AggId, Agg, Cmd, Event, Response] {

  val aggregateId: AggId
  protected val region: ActorRef
  protected val tracer: Tracer

  private implicit val ec: ExecutionContext = ExecutionContext.global

  def getState: Future[Option[Agg]] = {
    queryState
  }

  def applyEvent(event: Event): Future[ApplyEventResult[Response]] = {
    val envelope = PersistentActor.ApplyEvent[Event](aggregateId.toString, event)
    doApplyEvents(envelope).map(aggOpt => ApplyEventSuccess[Response](aggOpt)).recover { case e =>
      ApplyEventFailure[Response](e)
    }
  }
}
