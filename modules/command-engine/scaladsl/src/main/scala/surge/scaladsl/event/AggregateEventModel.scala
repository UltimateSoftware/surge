// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import surge.core.event.AggregateEventModelCoreTrait
import surge.internal.domain.{ AsyncEventHandler, EventHandler }
import surge.internal.persistence
import surge.scaladsl.common.Context

import scala.concurrent.Future

trait AggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvent(ctx: Context, state: Option[Agg], event: Evt): Option[Agg]

  override def toCore: EventHandler[Agg, Evt] = (ctx: persistence.Context, state: Option[Agg], event: Evt) => handleEvent(Context(ctx), state, event)
}

trait AsyncAggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvent(ctx: Context, state: Option[Agg], event: Evt): Future[Option[Agg]]

  override final def toCore: EventHandler[Agg, Evt] = new AsyncEventHandler[Agg, Evt] {
    override def handleEventAsync(ctx: persistence.Context, state: Option[Agg], event: Evt): Future[Option[Agg]] =
      AsyncAggregateEventModel.this.handleEvent(Context(ctx), state, event)
  }
}
