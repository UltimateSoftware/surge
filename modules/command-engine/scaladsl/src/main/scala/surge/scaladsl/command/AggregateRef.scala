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

  final def applyEvent(event: Event): Future[ApplyEventResult[Agg]] = applyEvents(List(event))
  def applyEvents(events: Seq[Event]): Future[ApplyEventResult[Agg]]
}

final class AggregateRefImpl[AggId, Agg, Cmd, Event](
    val aggregateId: AggId,
    protected val region: ActorRef,
    protected val tracer: Tracer,
    getEngineStatus: () => SurgeEngineStatus)(implicit val ec: ExecutionContext)
    extends AggregateRef[Agg, Cmd, Event]
    with AggregateRefBaseTrait[AggId, Agg, Cmd, Event]
    with AggregateRefTrait[AggId, Agg, Cmd, Event] {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  override def getState: Future[Option[Agg]] = {
    val status = getEngineStatus()

    if (status == SurgeEngineStatus.Running) {
      queryState
    } else {
      log.error(s"Engine Status: $status")
      Future.failed(SurgeEngineNotRunningException("The engine is not running, please call .start() on the engine before interacting with it"))
    }
  }

  def sendCommand(command: Cmd): Future[CommandResult[Agg]] = {
    val status = getEngineStatus()

    if (status == SurgeEngineStatus.Running) {
      val envelope = PersistentActor.ProcessMessage[Cmd](aggregateId.toString, command)
      sendCommand(envelope).map(aggOpt => CommandSuccess[Agg](aggOpt)).recover { case error: Throwable =>
        CommandFailure[Agg](error)
      }
    } else {
      log.error(s"Engine Status: $getEngineStatus")
      Future.failed(SurgeEngineNotRunningException("The engine is not running, please call .start() on the engine before interacting with it"))
    }
  }

}
