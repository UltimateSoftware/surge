// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import surge.core.event.AggregateEventModelCoreTrait
import surge.internal
import surge.internal.domain.EventHandler
import surge.internal.persistence
import surge.scaladsl.common.Context

trait AggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvent(ctx: Context, state: Option[Agg], event: Evt): Option[Agg]

  override def toCore: EventHandler[Agg, Evt] = (ctx: persistence.Context, state: Option[Agg], event: Evt) => handleEvent(Context(ctx), state, event)
}
