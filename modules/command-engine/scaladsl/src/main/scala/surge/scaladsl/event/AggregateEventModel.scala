// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.event

import surge.core.event.AggregateEventModelCoreTrait
import surge.internal.domain.EventHandler
import surge.internal.persistence
import surge.scaladsl.common.Context

trait AggregateEventModel[Agg, Evt, Response] extends AggregateEventModelCoreTrait[Agg, Evt, Response] {
  def handleEvent(ctx: Context, state: Option[Agg], event: Evt): Option[Agg]
  def extractResponse(agg: Option[Agg]): Option[Response]

  override def toCore: EventHandler[Agg, Evt, Response] = new EventHandler[Agg, Evt, Response] {
    override def handleEvent(ctx: persistence.Context, state: Option[Agg], event: Evt): Option[Agg] =
      AggregateEventModel.this.handleEvent(Context(ctx), state, event)
    override def extractResponse(state: Option[Agg]): Option[Response] = AggregateEventModel.this.extractResponse(state)
  }
}
