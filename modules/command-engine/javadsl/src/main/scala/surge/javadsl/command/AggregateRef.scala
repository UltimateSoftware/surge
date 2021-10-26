// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import org.slf4j.{ Logger, LoggerFactory }
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }
import surge.javadsl.common.{ AggregateRefBaseTrait, _ }
import surge.kafka.streams.SurgeHealthCheck

import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ ExecutionContext, Future }

trait AggregateRef[Agg, Cmd, Event] {
  def getState: CompletionStage[Optional[Agg]]

  def sendCommand(command: Cmd): CompletionStage[CommandResult[Agg]]

  def applyEvent(event: Event): CompletionStage[ApplyEventResult[Agg]]
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

  override def getState: CompletionStage[Optional[Agg]] = {
    val healthCheckFut = surgeHealthCheck.healthCheck()
    log.info("surge health check in process..")

    val queryStateResult = healthCheckFut.flatMap { health =>
      health.isHealthy match {
        case Some(true) =>
          log.info(s"The engine is up and running")
          queryState
        case None =>
          log.error(s"The engine is not running")
          Future.failed(new Exception("The engine is not running"))
      }
    }
    FutureConverters.toJava(queryStateResult.map(_.asJava))
  }

  def sendCommand(command: Cmd): CompletionStage[CommandResult[Agg]] = {
    val healthCheckFut = surgeHealthCheck.healthCheck()
    log.info("surge health check in process..")

    val result = healthCheckFut.flatMap { health =>
      health.isHealthy match {
        case Some(true) =>
          log.info(s"The engine is up and running")
          val envelope = PersistentActor.ProcessMessage[Cmd](aggregateId.toString, command)
          sendCommand(envelope).map(aggOpt => CommandSuccess[Agg](aggOpt.asJava)).recover { case error =>
            CommandFailure[Agg](error)
          }
        case None =>
          log.error(s"The engine is not running")
          Future.failed(new Exception("The engine is not running"))
      }
    }
    FutureConverters.toJava(result)
  }
}
