// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import surge.core.commondsl.SurgeProcessingModelCoreTrait
import surge.internal.domain.{ SurgeContext, SurgeProcessingModel }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateEventModel[Agg, Evt] extends SurgeProcessingModelCoreTrait[Agg, Nothing, Evt] {
  def handleEvents(state: Option[Agg], events: Seq[Evt]): Option[Agg]

  override def toCore: SurgeProcessingModel[Agg, Nothing, Evt] = new SurgeProcessingModel[Agg, Nothing, Evt] {
    override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Nothing)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
      throw new UnsupportedOperationException("Should not attempt to handle commands via AggregateEventModel")
    }

    override def applyAsync(ctx: SurgeContext[Agg, Evt], state: Option[Agg], events: Seq[Evt]): Future[SurgeContext[Agg, Evt]] = {
      Future.successful(ctx.updateState(handleEvents(state, events)).reply(state => state))
    }
  }
}

trait AsyncAggregateEventModel[Agg, Evt] extends SurgeProcessingModelCoreTrait[Agg, Nothing, Evt] {
  def handleEvents(state: Option[Agg], events: Seq[Evt]): Future[Option[Agg]]

  override def toCore: SurgeProcessingModel[Agg, Nothing, Evt] = new SurgeProcessingModel[Agg, Nothing, Evt] {
    override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Nothing)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
      throw new UnsupportedOperationException("Should not attempt to handle commands via AggregateEventModel")
    }

    override def applyAsync(ctx: SurgeContext[Agg, Evt], state: Option[Agg], events: Seq[Evt]): Future[SurgeContext[Agg, Evt]] = {
      handleEvents(state, events).map { newState =>
        ctx.updateState(newState).reply(state => state)
      }(ExecutionContext.global)
    }
  }
}
