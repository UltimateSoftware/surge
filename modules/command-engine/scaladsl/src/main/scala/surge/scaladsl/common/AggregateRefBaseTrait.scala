// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.common

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRefBaseTrait[AggId, Agg, Cmd, Event] extends AggregateRefTrait[AggId, Agg, Cmd, Event] {

  val aggregateId: AggId
  protected val region: ActorRef
  protected val tracer: Tracer

  private implicit val ec: ExecutionContext = ExecutionContext.global

  def getState: Future[Option[Agg]] = {
    queryState
  }

  def applyEvents(events: Seq[Event]): Future[ApplyEventResult[Agg]] = {
    val envelope = PersistentActor.ApplyEvents[Event](aggregateId.toString, events)
    applyEvents(envelope).map(aggOpt => ApplyEventSuccess[Agg](aggOpt)).recover { case e =>
      ApplyEventFailure[Agg](e)
    }
  }
}
