// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import surge.core.command.AggregateCommandModelCoreTrait
import surge.core.{ Context => CoreContext }
import surge.internal.domain.CommandHandler
import surge.javadsl._
import surge.javadsl.common.Context

import java.util.concurrent.CompletableFuture
import java.util.{ Optional, List => JList }
import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

trait AggregateCommandModel[Agg, Cmd, Evt] extends AggregateCommandModelCoreTrait[Agg, Cmd, Nothing, Evt] {
  def processCommand(aggregate: Optional[Agg], command: Cmd): JList[Evt]
  def handleEvent(aggregate: Optional[Agg], event: Evt): Optional[Agg]

  final def toCore: CommandHandler[Agg, Cmd, Nothing, Evt] =
    new CommandHandler[Agg, Cmd, Nothing, Evt] {
      override def processCommand(ctx: CoreContext, state: Option[Agg], cmd: Cmd): Future[CommandResult] =
        Future.successful(Right(AggregateCommandModel.this.processCommand(state.asJava, cmd).asScala.toSeq))
      override def apply(ctx: CoreContext, state: Option[Agg], event: Evt): Option[Agg] = handleEvent(state.asJava, event).asScala
    }
}

trait ContextAwareAggregateCommandModel[Agg, Cmd, Evt] extends AggregateCommandModelCoreTrait[Agg, Cmd, Nothing, Evt] {
  def processCommand(ctx: common.Context, aggregate: Optional[Agg], command: Cmd): CompletableFuture[Seq[Evt]]
  def handleEvent(ctx: common.Context, aggregate: Optional[Agg], event: Evt): Optional[Agg]

  final def toCore: CommandHandler[Agg, Cmd, Nothing, Evt] =
    new CommandHandler[Agg, Cmd, Nothing, Evt] {
      override def processCommand(ctx: CoreContext, state: Option[Agg], cmd: Cmd): Future[CommandResult] =
        FutureConverters
          .toScala(ContextAwareAggregateCommandModel.this.processCommand(Context(ctx), state.asJava, cmd))
          .map(v => Right(v))(ctx.executionContext)
      override def apply(ctx: CoreContext, state: Option[Agg], event: Evt): Option[Agg] = handleEvent(Context(ctx), state.asJava, event).asScala
    }
}

/**
 * Trait for implementing a rejectable command model. Unlike `AggregateCommandModel` this type of command model may choose to accept or reject commands.
 * @tparam Agg
 *   state type
 * @tparam Cmd
 *   command type
 * @tparam Rej
 *   rejection type
 * @tparam Evt
 *   event type
 */
trait RejectableAggregateCommandModel[Agg, Cmd, Rej, Evt] extends AggregateCommandModelCoreTrait[Agg, Cmd, Rej, Evt] {

  /**
   * Process a command
   * @param ctx
   *   the surge context
   * @param aggregate
   *   the current aggregate state
   * @param command
   *   the command to process
   * @return
   *   a CompleteableFuture of Either a rejection or a sequence or events generated by processing the command
   */
  def processCommand(ctx: Context, aggregate: Optional[Agg], command: Cmd): CompletableFuture[Either[Rej, Seq[Evt]]]

  /**
   * Handle an event
   * @param ctx
   *   the surge context
   * @param aggregate
   *   the current aggregate state
   * @param event
   *   the event to apply
   * @return
   *   the some resulting aggregate state or `None` to remove the state.
   */
  def handleEvent(ctx: Context, aggregate: Optional[Agg], event: Evt): Optional[Agg]

  final def toCore: CommandHandler[Agg, Cmd, Rej, Evt] =
    new CommandHandler[Agg, Cmd, Rej, Evt] {
      override def processCommand(ctx: CoreContext, state: Option[Agg], cmd: Cmd): Future[CommandResult] =
        FutureConverters.toScala(RejectableAggregateCommandModel.this.processCommand(Context(ctx), state.asJava, cmd))
      override def apply(ctx: CoreContext, state: Option[Agg], event: Evt): Option[Agg] = handleEvent(Context(ctx), state.asJava, event).asScala
    }
}
