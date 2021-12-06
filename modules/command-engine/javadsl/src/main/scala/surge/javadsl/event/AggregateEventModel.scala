// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import surge.core.commondsl.SurgeProcessingModelCoreTrait
import surge.internal.domain.{ SurgeContext, SurgeProcessingModel }

import java.util.Optional
import java.util.concurrent.CompletableFuture
import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._

trait AggregateEventModel[Agg, Evt] extends SurgeProcessingModelCoreTrait[Agg, Nothing, Evt] {
  def handleEvents(state: Optional[Agg], events: java.util.List[Evt]): Optional[Agg]

  override def toCore: SurgeProcessingModel[Agg, Nothing, Evt] = new SurgeProcessingModel[Agg, Nothing, Evt] {
    override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Nothing)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
      throw new UnsupportedOperationException("Should not attempt to handle commands via AggregateEventModel")
    }

    override def applyAsync(ctx: SurgeContext[Agg, Evt], state: Option[Agg], events: Seq[Evt]): Future[SurgeContext[Agg, Evt]] = {
      Future.successful(ctx.updateState(handleEvents(state.asJava, events.asJava).asScala).reply(state => state))
    }
  }
}

trait AsyncAggregateEventModel[Agg, Evt] extends SurgeProcessingModelCoreTrait[Agg, Nothing, Evt] {
  def handleEvents(state: Optional[Agg], event: java.util.List[Evt]): CompletableFuture[Optional[Agg]]

  override def toCore: SurgeProcessingModel[Agg, Nothing, Evt] = new SurgeProcessingModel[Agg, Nothing, Evt] {
    override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Nothing)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
      throw new UnsupportedOperationException("Should not attempt to handle commands via AggregateEventModel")
    }

    override def applyAsync(ctx: SurgeContext[Agg, Evt], state: Option[Agg], events: Seq[Evt]): Future[SurgeContext[Agg, Evt]] = {
      FutureConverters
        .toScala(handleEvents(state.asJava, events.asJava))
        .map { newState =>
          ctx.updateState(newState.asScala).reply(state => state)
        }(ExecutionContext.global)
    }
  }
}
