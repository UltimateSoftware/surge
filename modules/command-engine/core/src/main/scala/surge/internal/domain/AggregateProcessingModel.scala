// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import surge.internal.persistence.Context

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

/**
 * The result of handling a message
 * @param resultingState
 *   the resulting state
 * @param eventsToLog
 *   CQRS events to log.
 */
case class HandledMessageResult[S, +E](resultingState: Option[S], eventsToLog: Seq[E])

/**
 * AggregateProcessingModel defines a structure for a domain's types and algebra for an CommandService or EventService
 *
 * @tparam S
 *   State type
 * @tparam M
 *   Message type
 * @tparam R
 *   Rejection type
 * @tparam E
 *   Event type
 */

trait AggregateProcessingModel[S, M, +R, E] {

  /**
   * Process a message. Apply msg to state. Return either a rejection or a HandledMessageResult which is a resulting state and a sequence of events to be
   * persisted to the CQRS event topic.
   * @param ctx
   *   a context object for interacting with the X-engine.
   * @param state
   *   the current state of the aggregate
   * @param msg
   *   the message
   * @return
   *   the future result of processing the message
   */
  def handle(ctx: Context, state: Option[S], msg: M): Future[Either[R, HandledMessageResult[S, E]]]

  def applyAsync(ctx: Context, state: Option[S], events: Seq[E]): Future[Option[S]]
}

trait SynchronousEventHandler[S, E] {
  this: AggregateProcessingModel[S, _, _, E] =>
  def apply(ctx: Context, state: Option[S], event: E): Option[S]

  override def applyAsync(ctx: Context, state: Option[S], events: Seq[E]): Future[Option[S]] =
    events.foldLeft(Future.successful(state))((s: Future[Option[S]], e: E) => s.flatMap(prev => Future.fromTry(Try(apply(ctx, prev, e))))(ctx.executionContext))
}

trait CommandHandler[S, M, R, E] extends AggregateProcessingModel[S, M, R, E] with SynchronousEventHandler[S, E] {
  type CommandResult = Either[R, Seq[E]]

  def processCommand(ctx: Context, state: Option[S], cmd: M): Future[CommandResult]

  override def handle(ctx: Context, state: Option[S], cmd: M): Future[Either[R, HandledMessageResult[S, E]]] = {
    implicit val ec: ExecutionContext = ctx.executionContext
    processCommand(ctx, state, cmd).flatMap {
      case Left(rejected) => Future.successful(Left(rejected))
      case Right(events) =>
        applyAsync(ctx, state, events).map { resultingState =>
          Right(HandledMessageResult(resultingState = resultingState, eventsToLog = events))
        }
    }
  }
}

trait AsyncCommandHandler[S, M, R, E] extends CommandHandler[S, M, R, E] {

  override final def apply(ctx: Context, state: Option[S], event: E): Option[S] =
    throw new Exception("Synchronous event handler called when using AsyncCommandHandler. This should never happen")

  def applyAsync(ctx: Context, initialState: Option[S], events: Seq[E]): Future[Option[S]]

  override final def handle(ctx: Context, state: Option[S], cmd: M): Future[Either[R, HandledMessageResult[S, E]]] = {
    implicit val ec: ExecutionContext = ctx.executionContext
    processCommand(ctx, state, cmd).flatMap {
      case Left(rejected) => Future.successful(Left(rejected))
      case Right(events) =>
        applyAsync(ctx, state, events).map { maybeS =>
          Right(HandledMessageResult(maybeS, events))
        }
    }
  }
}

trait EventHandler[S, E] extends AggregateProcessingModel[S, Nothing, Nothing, E] with SynchronousEventHandler[S, E] {
  def handleEvents(ctx: Context, state: Option[S], event: Seq[E]): Option[S]

  override def apply(ctx: Context, state: Option[S], event: E): Option[S] = handleEvents(ctx, state, List(event))

  override final def handle(ctx: Context, state: Option[S], msg: Nothing): Future[Either[Nothing, HandledMessageResult[S, Nothing]]] =
    throw new RuntimeException("Impossible")

  override def applyAsync(ctx: Context, state: Option[S], events: Seq[E]): Future[Option[S]] =
    Future.fromTry(Try(handleEvents(ctx, state, events)))
}

trait AsyncEventHandler[S, E] extends EventHandler[S, E] {
  def handleEventsAsync(ctx: Context, state: Option[S], events: Seq[E]): Future[Option[S]]

  override final def handleEvents(ctx: Context, state: Option[S], events: Seq[E]): Option[S] =
    throw new Exception("Synchronous event handler called when using AsyncEventHandler. This should never happen")

  override def apply(ctx: Context, state: Option[S], event: E): Option[S] =
    throw new Exception("Synchronous event application called when using AsyncEventHandler. This should never happen")

  override final def applyAsync(ctx: Context, state: Option[S], events: Seq[E]): Future[Option[S]] =
    handleEventsAsync(ctx, state, events)
}
