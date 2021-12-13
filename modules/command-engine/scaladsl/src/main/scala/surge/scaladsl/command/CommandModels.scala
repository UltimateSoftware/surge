// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import surge.core.commondsl.SurgeProcessingModelCoreTrait
import surge.internal.domain.{ SurgeContext, SurgeProcessingModel }
import surge.scaladsl.common.Context

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

trait AggregateCommandModel[Agg, Cmd, Evt] extends SurgeProcessingModelCoreTrait[Agg, Cmd, Evt] {
  def processCommand(aggregate: Option[Agg], command: Cmd): Try[Seq[Evt]]
  def handleEvent(aggregate: Option[Agg], event: Evt): Option[Agg]

  final def toCore: SurgeProcessingModel[Agg, Cmd, Evt] = {
    new SurgeProcessingModel[Agg, Cmd, Evt] {
      override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Cmd)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
        Future.fromTry(processCommand(state, msg)).map { events =>
          val newState = events.foldLeft(state)((s: Option[Agg], e: Evt) => handleEvent(s, e))
          ctx.persistEvents(events).updateState(newState).reply(state => state)
        }
      }

      override def applyAsync(ctx: SurgeContext[Agg, Evt], state: Option[Agg], events: Seq[Evt]): Future[SurgeContext[Agg, Evt]] = {
        val newState = events.foldLeft(state)((stateAccum, evt) => handleEvent(stateAccum, evt))
        Future.fromTry(Try(ctx.updateState(newState).reply(state => state)))
      }
    }
  }
}

trait AsyncAggregateCommandModel[Agg, Cmd, Evt] extends SurgeProcessingModelCoreTrait[Agg, Cmd, Evt] {
  def executionContext: ExecutionContext
  def processCommand(aggregate: Option[Agg], command: Cmd): Future[Seq[Evt]]
  def handleEvents(aggregate: Option[Agg], event: Seq[Evt]): Future[Option[Agg]]

  final def toCore: SurgeProcessingModel[Agg, Cmd, Evt] = {
    new SurgeProcessingModel[Agg, Cmd, Evt] {
      override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Cmd)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
        processCommand(state, msg).flatMap { events =>
          handleEvents(state, events).map { newState =>
            ctx.persistEvents(events).updateState(newState).reply(state => state)
          }
        }
      }

      // FIXME Does applyEvents make sense any more? In the core model state updates should happen via the handle method now.
      //  We probably still do need something for applying events directly though...
      override def applyAsync(ctx: SurgeContext[Agg, Evt], state: Option[Agg], events: Seq[Evt]): Future[SurgeContext[Agg, Evt]] = {
        handleEvents(state, events).map { newState =>
          ctx.updateState(newState).reply(state => state)
        }(ExecutionContext.global)
      }
    }
  }
}

trait ContextAwareAggregateCommandModel[Agg, Cmd, Evt] extends SurgeProcessingModelCoreTrait[Agg, Cmd, Evt] {
  def processCommand(ctx: Context[Agg, Evt], aggregate: Option[Agg], command: Cmd): Future[Context[Agg, Evt]]
  def handleEvent(aggregate: Option[Agg], event: Evt): Option[Agg]

  final def toCore: SurgeProcessingModel[Agg, Cmd, Evt] =
    new SurgeProcessingModel[Agg, Cmd, Evt] {
      override def handle(ctx: SurgeContext[Agg, Evt], state: Option[Agg], msg: Cmd)(implicit ec: ExecutionContext): Future[SurgeContext[Agg, Evt]] = {
        processCommand(Context(ctx), state, msg).map(_.toCore)
      }

      override def applyAsync(ctx: SurgeContext[Agg, Evt], state: Option[Agg], events: Seq[Evt]): Future[SurgeContext[Agg, Evt]] = {
        val newState = events.foldLeft(state)((stateAccum, evt) => handleEvent(stateAccum, evt))
        Future.successful(ctx.updateState(newState).reply(s => s))
      }
    }
}
