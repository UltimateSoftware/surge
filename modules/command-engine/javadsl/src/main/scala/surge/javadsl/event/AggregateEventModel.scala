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
import scala.jdk.CollectionConverters.SeqHasAsJava

trait AggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvents(ctx: Context, state: Optional[Agg], events: java.util.List[Evt]): Optional[Agg]

  override def toCore: EventHandler[Agg, Evt] = (ctx: persistence.Context, state: Option[Agg], events: Seq[Evt]) =>
    handleEvents(Context(ctx), state.asJava, events.asJava).asScala
}

trait AsyncAggregateEventModel[Agg, Evt] extends AggregateEventModelCoreTrait[Agg, Evt] {
  def handleEvents(ctx: Context, state: Optional[Agg], events: java.util.List[Evt]): CompletableFuture[Optional[Agg]]

  override final def toCore: EventHandler[Agg, Evt] = new AsyncEventHandler[Agg, Evt] {
    override def handleEventsAsync(ctx: persistence.Context, state: Option[Agg], events: Seq[Evt]): Future[Option[Agg]] =
      FutureConverters.toScala(AsyncAggregateEventModel.this.handleEvents(Context(ctx), state.asJava, events.asJava)).map(_.asScala)(ctx.executionContext)
  }
}
