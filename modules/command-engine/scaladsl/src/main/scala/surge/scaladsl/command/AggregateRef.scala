// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import org.slf4j.{ Logger, LoggerFactory }
import surge.exceptions.SurgeEngineNotRunningException
import surge.internal.domain.{ SurgeEngineStatus, SurgeMessagePipeline }
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }
import surge.scaladsl.common.{ AggregateRefBaseTrait, _ }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRef[Agg, Cmd, Event] {
  def getState: Future[Option[Agg]]

  def sendCommand(command: Cmd): Future[CommandResult[Agg]]

  def applyEvents(event: Seq[Event]): Future[ApplyEventResult[Agg]]
}

final class AggregateRefImpl[AggId, Agg, Cmd, Event](val aggregateId: AggId, protected val region: ActorRef, protected val tracer: Tracer)
    extends AggregateRef[Agg, Cmd, Event]
    with AggregateRefBaseTrait[AggId, Agg, Cmd, Event]
    with AggregateRefTrait[AggId, Agg, Cmd, Event] {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val log: Logger = LoggerFactory.getLogger(getClass)

  override def getState: Future[Option[Agg]] = {
    val engineStatus = SurgeMessagePipeline.surgeEngineStatus

    if (engineStatus == SurgeEngineStatus.Running) {
      queryState
    } else {
      log.error(s"Engine Status: $engineStatus")
      Future.failed(SurgeEngineNotRunningException("The engine is not running, please call .start() on the engine before interacting with it"))
    }
  }

  def sendCommand(command: Cmd): Future[CommandResult[Agg]] = {
    val engineStatus = SurgeMessagePipeline.surgeEngineStatus

    if (engineStatus == SurgeEngineStatus.Running) {
      val envelope = PersistentActor.ProcessMessage[Cmd](aggregateId.toString, command)
      sendCommand(envelope).map(aggOpt => CommandSuccess[Agg](aggOpt)).recover { case error: Throwable =>
        CommandFailure[Agg](error)
      }
    } else {
      log.error(s"Engine Status: $engineStatus")
      Future.failed(SurgeEngineNotRunningException("The engine is not running, please call .start() on the engine before interacting with it"))
    }
  }

}
