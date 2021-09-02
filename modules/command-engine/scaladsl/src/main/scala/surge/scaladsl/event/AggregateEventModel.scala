// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import surge.core.event.AggregateEventModelCoreTrait
import surge.internal.domain.EventHandler
import surge.internal.persistence
import surge.scaladsl.common.Context

trait AggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt, Agg] {
  def handleEvent(ctx: Context, state: Option[Agg], event: Evt): Option[Agg]

  override def toCore: EventHandler[Agg, Evt, Agg] = new EventHandler[Agg, Evt, Agg] {
    override def handleEvent(ctx: persistence.Context, state: Option[Agg], event: Evt): Option[Agg] =
      AggregateEventModel.this.handleEvent(Context(ctx), state, event)

    override def extractResponse(state: Option[Agg]): Option[Agg] = state
  }
}
