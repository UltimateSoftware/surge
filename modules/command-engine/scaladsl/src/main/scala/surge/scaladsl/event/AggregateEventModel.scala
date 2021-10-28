// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import surge.core.event.AggregateEventModelCoreTrait
import surge.internal.domain.{ AsyncEventHandler, EventHandler }
import surge.internal.persistence
import surge.scaladsl.common.Context

import scala.concurrent.Future

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
