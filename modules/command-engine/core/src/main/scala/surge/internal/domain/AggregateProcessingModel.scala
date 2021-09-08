// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import surge.internal.persistence.Context

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

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
   * @param ec
   *   an execution context for generating futures
   * @return
   *   the future result of processing the message
   */
  def handle(ctx: Context, state: Option[S], msg: M)(implicit ec: ExecutionContext): Future[Either[R, HandledMessageResult[S, E]]]

  /**
   * Apply an event. Apply event to state and return the resulting state.
   * @param ctx
   *   a context object for interacting with the X-engine.
   * @param state
   *   the current state of the aggregate
   * @param event
   *   the event to apply
   * @return
   *   the resulting state
   */
  def apply(ctx: Context, state: Option[S], event: E): Option[S]

  def applyAsync(ctx: Context, state: Option[S], event: E): Future[Option[S]] = {
    Try(apply(ctx, state, event)) match {
      case Failure(exception) => Future.failed(exception)
      case Success(value)     => Future.successful(value)
    }
  }

}

trait CommandHandler[S, M, R, E] extends AggregateProcessingModel[S, M, R, E] {
  type CommandResult = Either[R, Seq[E]]

  def processCommand(ctx: Context, state: Option[S], cmd: M): Future[CommandResult]

  override def handle(ctx: Context, state: Option[S], cmd: M)(implicit ec: ExecutionContext): Future[Either[R, HandledMessageResult[S, E]]] =
    processCommand(ctx, state, cmd).map {
      case Left(rejected) => Left(rejected)
      case Right(events) =>
        Right(HandledMessageResult(events.foldLeft(state)((s: Option[S], e: E) => apply(ctx, s, e)), events))
    }
}

trait AsyncCommandHandler[S, M, R, E] extends CommandHandler[S, M, R, E] {

  def processCommand(ctx: Context, state: Option[S], cmd: M): Future[CommandResult]

  override def apply(ctx: Context, state: Option[S], event: E): Option[S] =
    throw new Exception("Synchronous event handler called when using AsyncCommandHandler. This should never happen")

  // equivalent to a fold left but executed remotely
  def applyAsync(ctx: Context, initialState: Option[S], events: Seq[E]): Future[Option[S]]

  override def applyAsync(ctx: Context, state: Option[S], event: E): Future[Option[S]] =
    applyAsync(ctx, state, Seq(event))

  override final def handle(ctx: Context, state: Option[S], cmd: M)(implicit ec: ExecutionContext): Future[Either[R, HandledMessageResult[S, E]]] =
    processCommand(ctx, state, cmd).flatMap {
      case Left(rejected) => Future.successful(Left(rejected))
      case Right(events) =>
        applyAsync(ctx, state, events).map { maybeS =>
          Right(HandledMessageResult(maybeS, events))
        }
    }
}

trait EventHandler[S, E] extends AggregateProcessingModel[S, Nothing, Nothing, E] {
  def handleEvent(ctx: Context, state: Option[S], event: E): Option[S]

  override final def handle(ctx: Context, state: Option[S], msg: Nothing)(
      implicit ec: ExecutionContext): Future[Either[Nothing, HandledMessageResult[S, Nothing]]] =
    throw new RuntimeException("Impossible")

  override final def apply(ctx: Context, state: Option[S], event: E): Option[S] = handleEvent(ctx, state, event)
}
