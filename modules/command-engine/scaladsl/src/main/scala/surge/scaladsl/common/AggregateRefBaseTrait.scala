// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.common

import akka.actor.ActorRef
import io.opentracing.Tracer
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRefEvent[Agg, Event] {
  def getState: Future[Option[Agg]]
  def applyEvent(event: Event): Future[ApplyEventResult[Agg]]
}

trait AggregateRefBaseTrait[AggId, Agg, Cmd, Event] extends AggregateRefEvent[Agg, Event] with AggregateRefTrait[AggId, Agg, Cmd, Event] {

  val aggregateId: AggId
  protected val region: ActorRef
  protected val tracer: Tracer

  private implicit val ec: ExecutionContext = ExecutionContext.global

  def getState: Future[Option[Agg]] = {
    queryState
  }

  def applyEvent(event: Event): Future[ApplyEventResult[Agg]] = {
    val envelope = PersistentActor.ApplyEvent[Event](aggregateId.toString, event)
    val result = applyEventsWithRetries(envelope).map(aggOpt => ApplyEventSuccess[Agg](aggOpt)).recover { case e =>
      ApplyEventFailure[Agg](e)
    }
    result
  }
}
