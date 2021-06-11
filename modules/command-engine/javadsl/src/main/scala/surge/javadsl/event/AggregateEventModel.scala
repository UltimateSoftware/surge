// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import surge.core.event.AggregateEventModelCoreTrait
import surge.internal
import surge.internal.domain.EventHandler
import surge.internal.persistence
import surge.javadsl.common.Context

import java.util.Optional
import scala.compat.java8.OptionConverters._

trait AggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvent(ctx: Context, state: Optional[Agg], event: Evt): Optional[Agg]

  override def toCore: EventHandler[Agg, Evt] = (ctx: persistence.Context, state: Option[Agg], event: Evt) =>
    handleEvent(Context(ctx), state.asJava, event).asScala
}
