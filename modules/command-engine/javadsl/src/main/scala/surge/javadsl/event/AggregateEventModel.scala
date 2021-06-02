// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import surge.core.Context
import surge.core.event.AggregateEventModelCoreTrait
import surge.internal.domain.EventHandler

import java.util.Optional
import scala.compat.java8.OptionConverters._

trait AggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvent(ctx: Context, state: Optional[Agg], event: Evt): Optional[Agg]

  override def toCore: EventHandler[Agg, Evt] = (ctx: Context, state: Option[Agg], event: Evt) => handleEvent(ctx, state.asJava, event).asScala
}
