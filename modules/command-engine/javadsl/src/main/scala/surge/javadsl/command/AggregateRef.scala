// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import org.slf4j.{ Logger, LoggerFactory }
import surge.exceptions.SurgeEngineNotRunningException
import surge.internal.domain.{ SurgeEngineStatus, SurgeMessagePipeline }
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }
import surge.javadsl.common.{ AggregateRefBaseTrait, _ }

import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRef[Agg, Cmd, Event] {
  def getState: CompletionStage[Optional[Agg]]

  def sendCommand(command: Cmd): CompletionStage[CommandResult[Agg]]

  final def applyEvent(event: Event): CompletionStage[ApplyEventResult[Agg]] = applyEvents(List(event))
  def applyEvents(events: Seq[Event]): CompletionStage[ApplyEventResult[Agg]]
}

final class AggregateRefImpl[AggId, Agg, Cmd, Event](val aggregateId: AggId, protected val region: ActorRef, protected val tracer: Tracer)
    extends AggregateRef[Agg, Cmd, Event]
    with AggregateRefBaseTrait[AggId, Agg, Cmd, Event]
    with AggregateRefTrait[AggId, Agg, Cmd, Event] {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val log: Logger = LoggerFactory.getLogger(getClass)

  override def getState: CompletionStage[Optional[Agg]] = {
    val engineStatus = SurgeMessagePipeline.surgeEngineStatus

    val result = if (engineStatus == SurgeEngineStatus.Running) {
      queryState
    } else {
      log.error(s"Engine Status: $engineStatus")
      Future.failed(SurgeEngineNotRunningException("The engine is not running, please call .start() on the engine before interacting with it"))
    }

    FutureConverters.toJava(result.map(_.asJava))
  }

  def sendCommand(command: Cmd): CompletionStage[CommandResult[Agg]] = {
    val engineStatus = SurgeMessagePipeline.surgeEngineStatus

    val result = if (engineStatus == SurgeEngineStatus.Running) {
      val envelope = PersistentActor.ProcessMessage[Cmd](aggregateId.toString, command)
      sendCommand(envelope).map(aggOpt => CommandSuccess[Agg](aggOpt.asJava)).recover { case error =>
        CommandFailure[Agg](error)
      }
    } else {
      log.error(s"Engine Status: $engineStatus")
      Future.failed(SurgeEngineNotRunningException("The engine is not running, please call .start() on the engine before interacting with it"))
    }

    FutureConverters.toJava(result)
  }
}
