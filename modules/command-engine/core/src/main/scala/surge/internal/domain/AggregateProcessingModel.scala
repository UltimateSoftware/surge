// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import surge.internal.persistence.Context

import scala.concurrent.{ ExecutionContext, Future }

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
 * @tparam State
 *   State type
 * @tparam Message
 *   Message type
 * @tparam Rejection
 *   Rejection type
 * @tparam Event
 *   Event type
 * @tparam Response
 *   Response type
 */

trait AggregateProcessingModel[State, Message, +Rejection, Event, Response] {

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
  def handle(ctx: Context, state: Option[State], msg: Message)(implicit ec: ExecutionContext): Future[Either[Rejection, HandledMessageResult[State, Event]]]

  /**
   * Apply an event. Apply event to state and return a future of the resulting state.
   * @param ctx
   *   a context object for interacting with the X-engine.
   * @param state
   *   the current state of the aggregate
   * @param event
   *   the event to apply
   * @return
   *   the future resulting state
   */
  def apply(ctx: Context, state: Option[State], event: Event): Option[State]

  /**
   * Extracts a response from the resulting state.
   * @param state
   *   The state of the aggregate after a comannd is successfully handled and any generated events are applied to the previous state
   * @return
   *   A response to send back to the original sender of the command
   */
  def extractResponse(state: Option[State]): Option[Response]

}

trait CommandHandler[State, Message, Rejection, Event, Response] extends AggregateProcessingModel[State, Message, Rejection, Event, Response] {
  type CommandResult = Either[Rejection, Seq[Event]]

  def processCommand(ctx: Context, state: Option[State], cmd: Message): Future[CommandResult]

  override final def handle(ctx: Context, state: Option[State], cmd: Message)(
      implicit ec: ExecutionContext): Future[Either[Rejection, HandledMessageResult[State, Event]]] =
    processCommand(ctx, state, cmd).map {
      case Left(rejected) => Left(rejected)
      case Right(events) =>
        Right(HandledMessageResult(events.foldLeft(state)((s: Option[State], e: Event) => apply(ctx, s, e)), events))
    }
}

trait EventHandler[State, Event, Response] extends AggregateProcessingModel[State, Nothing, Nothing, Event, Response] {
  def handleEvent(ctx: Context, state: Option[State], event: Event): Option[State]

  override final def handle(ctx: Context, state: Option[State], msg: Nothing)(
      implicit ec: ExecutionContext): Future[Either[Nothing, HandledMessageResult[State, Nothing]]] =
    throw new RuntimeException("Impossible")

  override final def apply(ctx: Context, state: Option[State], event: Event): Option[State] = handleEvent(ctx, state, event)
}
