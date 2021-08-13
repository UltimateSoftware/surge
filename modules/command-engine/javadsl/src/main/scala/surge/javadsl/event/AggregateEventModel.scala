// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import surge.core.event.AggregateEventModelCoreTrait
import surge.internal.domain.EventHandler
import surge.internal.persistence
import surge.javadsl.common.Context

import java.util.Optional
import scala.compat.java8.OptionConverters._

trait AggregateEventModel[Agg, Evt, Response] extends AggregateEventModelCoreTrait[Agg, Evt, Response] {
  def handleEvent(ctx: Context, state: Optional[Agg], event: Evt): Optional[Agg]
  def extractResponse(state: Optional[Agg]): Optional[Response]

  override def toCore: EventHandler[Agg, Evt, Response] = new EventHandler[Agg, Evt, Response] {
    override def handleEvent(ctx: persistence.Context, state: Option[Agg], event: Evt): Option[Agg] =
      AggregateEventModel.this.handleEvent(Context(ctx), state.asJava, event).asScala
    override def extractResponse(state: Option[Agg]): Option[Response] = AggregateEventModel.this.extractResponse(state.asJava).asScala
  }
}
