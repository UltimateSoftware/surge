// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import surge.core.event.AggregateEventModelCoreTrait
import surge.internal.domain.{ AsyncEventHandler, EventHandler }
import surge.internal.persistence
import surge.javadsl.common.Context

import java.util.Optional
import java.util.concurrent.CompletableFuture
import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ ExecutionContext, Future }

trait AggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvent(ctx: Context, state: Optional[Agg], event: Evt): Optional[Agg]

  override def toCore: EventHandler[Agg, Evt] = (ctx: persistence.Context, state: Option[Agg], event: Evt) =>
    handleEvent(Context(ctx), state.asJava, event).asScala
}

trait AsyncAggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvent(ctx: Context, state: Optional[Agg], event: Evt): CompletableFuture[Optional[Agg]]

  override final def toCore: EventHandler[Agg, Evt] = new AsyncEventHandler[Agg, Evt] {
    override def handleEventAsync(ctx: persistence.Context, state: Option[Agg], event: Evt): Future[Option[Agg]] =
      FutureConverters.toScala(AsyncAggregateEventModel.this.handleEvent(Context(ctx), state.asJava, event)).map(_.asScala)(ExecutionContext.global)
  }
}
