// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import org.slf4j.{ Logger, LoggerFactory }
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }
import surge.kafka.streams.SurgeHealthCheck
import surge.scaladsl.common.{ AggregateRefBaseTrait, _ }

import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRef[Agg, Cmd, Event] {
  def getState: Future[Option[Agg]]

  def sendCommand(command: Cmd): Future[CommandResult[Agg]]

  def applyEvent(event: Event): Future[ApplyEventResult[Agg]]
}

final class AggregateRefImpl[AggId, Agg, Cmd, Event](
    val aggregateId: AggId,
    protected val region: ActorRef,
    protected val tracer: Tracer,
    surgeHealthCheck: SurgeHealthCheck)
    extends AggregateRef[Agg, Cmd, Event]
    with AggregateRefBaseTrait[AggId, Agg, Cmd, Event]
    with AggregateRefTrait[AggId, Agg, Cmd, Event] {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val log: Logger = LoggerFactory.getLogger(getClass)

  override def getState: Future[Option[Agg]] = {
    val healthCheckFut = surgeHealthCheck.healthCheck()
    log.info("surge health check in process..")

    healthCheckFut.flatMap { health =>
      health.isHealthy match {
        case Some(true) =>
          log.info(s"The engine is up and running")
          queryState
        case None =>
          log.error(s"The engine is not running")
          Future.failed(new Exception("The engine is not running"))
      }
    }
  }

  def sendCommand(command: Cmd): Future[CommandResult[Agg]] = {
    val healthCheckFut = surgeHealthCheck.healthCheck()
    log.info("surge health check in process..")

    healthCheckFut.flatMap { health =>
      health.isHealthy match {
        case Some(true) =>
          log.info(s"The engine is up and running")
          val envelope = PersistentActor.ProcessMessage[Cmd](aggregateId.toString, command)
          sendCommand(envelope).map(aggOpt => CommandSuccess[Agg](aggOpt)).recover { case error: Throwable =>
            CommandFailure[Agg](error)
          }
        case None =>
          log.error(s"The engine is not running")
          Future.failed(new Exception("The engine is not running"))
      }
    }
  }
}
