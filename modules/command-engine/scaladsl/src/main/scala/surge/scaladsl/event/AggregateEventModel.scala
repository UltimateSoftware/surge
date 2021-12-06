// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import surge.core.commondsl.SurgeProcessingModelCoreTrait
import surge.internal.domain.{ SurgeContext, SurgeProcessingModel }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateEventModel[Agg, Evt] extends SurgeProcessingModelCoreTrait[Agg, Nothing, Evt] {
  def handleEvent(state: Option[Agg], event: Evt): Option[Agg]

  override def toCore: SurgeProcessingModel[Agg, Nothing, Evt] = new SurgeProcessingModel[Agg, Nothing, Evt] {
    override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Nothing)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
      throw new UnsupportedOperationException("Should not attempt to handle commands via AggregateEventModel")
    }
trait AggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvents(ctx: Context, state: Option[Agg], events: Seq[Evt]): Option[Agg]

  override def toCore: EventHandler[Agg, Evt] =
    (ctx: persistence.Context, state: Option[Agg], events: Seq[Evt]) => AggregateEventModel.this.handleEvents(Context(ctx), state, events)
}

trait AsyncAggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvents(ctx: Context, state: Option[Agg], events: Seq[Evt]): Future[Option[Agg]]

  override final def toCore: AsyncEventHandler[Agg, Evt] =
    (ctx: persistence.Context, state: Option[Agg], events: Seq[Evt]) => AsyncAggregateEventModel.this.handleEvents(Context(ctx), state, events)
}
