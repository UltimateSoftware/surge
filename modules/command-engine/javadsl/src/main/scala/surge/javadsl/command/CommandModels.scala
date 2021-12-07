// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import surge.core.commondsl.SurgeProcessingModelCoreTrait
import surge.internal.domain.{ SurgeContext, SurgeProcessingModel }
import surge.javadsl.common.Context

import java.util.concurrent.CompletableFuture
import java.util.{ Optional, List => JList }
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._
import scala.compat.java8.FutureConverters._
import scala.util.Try

trait AggregateCommandModel[Agg, Cmd, Evt] extends SurgeProcessingModelCoreTrait[Agg, Cmd, Evt] {
  def processCommand(aggregate: Optional[Agg], command: Cmd): JList[Evt]
  def handleEvent(aggregate: Optional[Agg], event: Evt): Optional[Agg]

  final def toCore: SurgeProcessingModel[Agg, Cmd, Evt] =
    new SurgeProcessingModel[Agg, Cmd, Evt] {

      private def newStateFromEvents(events: Seq[Evt], state: Option[Agg]): Optional[Agg] = {
        events.foldLeft(state.asJava)((s: Optional[Agg], e: Evt) => handleEvent(s, e))
      }

      override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Cmd)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
        Future.fromTry(Try(AggregateCommandModel.this.processCommand(state.asJava, msg).asScala.toSeq)).map { events =>
          val newState = newStateFromEvents(events, state)
          ctx.persistEvents(events).updateState(newState.asScala).reply(state => state)
        }
      }

      override def applyAsync(ctx: SurgeContext[Agg, Evt], state: Option[Agg], events: Seq[Evt]): Future[SurgeContext[Agg, Evt]] = {
        Future.fromTry(Try(ctx.updateState(newStateFromEvents(events, state).asScala).reply(state => state)))
      }
    }
}

trait AsyncAggregateCommandModel[Agg, Cmd, Evt] extends SurgeProcessingModelCoreTrait[Agg, Cmd, Evt] {
  def processCommand(aggregate: Optional[Agg], command: Cmd): CompletableFuture[JList[Evt]]
  def handleEvent(aggregate: Optional[Agg], event: Evt): CompletableFuture[Optional[Agg]]

  final def toCore: SurgeProcessingModel[Agg, Cmd, Evt] =
    new SurgeProcessingModel[Agg, Cmd, Evt] {
      private def newStateFromEvents(events: Seq[Evt], state: Option[Agg])(implicit ec: ExecutionContext): Future[Optional[Agg]] = {
        events.foldLeft(Future.successful(state.asJava))((fut, e) => fut.flatMap(s => handleEvent(s, e).toScala))
      }
      override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Cmd)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
        processCommand(state.asJava, msg).toScala.flatMap { events =>
          newStateFromEvents(events.asScala.toSeq, state).map { newState =>
            ctx.persistEvents(events.asScala.toSeq).updateState(newState.asScala).reply(state => state)
          }
        }
      }

      // FIXME Does applyEvents make sense any more? In the core model state updates should happen via the handle method now.
      //  We probably still do need something for applying events directly though...
      override def applyAsync(ctx: SurgeContext[Agg, Evt], state: Option[Agg], events: Seq[Evt]): Future[SurgeContext[Agg, Evt]] = {
        implicit val ec: ExecutionContext = ExecutionContext.global
        newStateFromEvents(events, state).map { newState =>
          ctx.updateState(newState.asScala).reply(state => state)
        }
      }
    }
}

trait ContextAwareAggregateCommandModel[Agg, Cmd, Evt] extends SurgeProcessingModelCoreTrait[Agg, Cmd, Evt] {
  def processCommand(ctx: Context[Agg, Evt], aggregate: Optional[Agg], command: Cmd): CompletableFuture[Context[Agg, Evt]]
  def handleEvent(aggregate: Optional[Agg], event: Evt): Optional[Agg]

  final def toCore: SurgeProcessingModel[Agg, Cmd, Evt] =
    new SurgeProcessingModel[Agg, Cmd, Evt] {
      override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Cmd)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
        processCommand(Context(ctx), state.asJava, msg).toScala.map(_.toCore)
      }

      override def applyAsync(ctx: SurgeContext[Agg, Evt], state: Option[Agg], events: Seq[Evt]): Future[SurgeContext[Agg, Evt]] = {
        val newState = events.foldLeft(state.asJava)((s: Optional[Agg], e: Evt) => handleEvent(s, e))
        Future.successful(ctx.updateState(newState.asScala).reply(s => s))
      }
    }
}

// TODO Add explicit reply type model to prove out the abstraction
