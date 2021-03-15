// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.actor.ActorRef
import surge.core.AggregateRefTrait
import surge.internal.persistence.cqrs.CQRSPersistentActor

import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext

trait AggregateRef[Agg, Cmd, Event] {
  def getState: CompletionStage[Optional[Agg]]
  def ask(command: Cmd): CompletionStage[CommandResult[Agg]]
  def applyEvent(event: Event): CompletionStage[ApplyEventResult[Agg]]
}

final class AggregateRefImpl[AggId, Agg, Cmd, Event](
    val aggregateId: AggId,
    val region: ActorRef) extends AggregateRef[Agg, Cmd, Event]
  with AggregateRefTrait[AggId, Agg, Cmd, Event] {

  private implicit val ec: ExecutionContext = ExecutionContext.global

  def getState: CompletionStage[Optional[Agg]] = {
    FutureConverters.toJava(queryState.map(_.asJava))
  }

  def ask(command: Cmd): CompletionStage[CommandResult[Agg]] = {
    val envelope = CQRSPersistentActor.CommandEnvelope[Cmd](aggregateId.toString, command)
    val result = askWithRetries(envelope).map {
      case Left(error) =>
        CommandFailure[Agg](error)
      case Right(aggOpt) =>
        CommandSuccess[Agg](aggOpt.asJava)
    }
    FutureConverters.toJava(result)
  }

  def applyEvent(event: Event): CompletionStage[ApplyEventResult[Agg]] = {
    val envelope = CQRSPersistentActor.ApplyEventEnvelope[Event](aggregateId.toString, event)
    val result = applyEventsWithRetries(envelope)
      .map(aggOpt => ApplyEventsSuccess[Agg](aggOpt.asJava))
      .recover {
        case e =>
          ApplyEventsFailure[Agg](e)
      }
    FutureConverters.toJava(result)
  }
}
