// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.grpc.GrpcClientSettings
import com.ukg.surge.multilanguage.protobuf.HealthCheckReply.Status
import com.ukg.surge.multilanguage.protobuf._
import surge.metrics.{ MetricInfo, Metrics, RecordingLevel, Timer }
import surge.scaladsl.command.SurgeCommand
import surge.scaladsl.common.{ CommandFailure, CommandSuccess }

import java.util.UUID
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class MultilanguageGatewayServiceImpl(surgeEngine: SurgeCommand[UUID, SurgeState, SurgeCmd, Nothing, SurgeEvent])(implicit system: ActorSystem)
    extends MultilanguageGatewayService {

  import Implicits._
  import system.dispatcher

  val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServiceImpl])

  private val metric: Metrics = Metrics.globalMetricRegistry
  private val forwardCommandTimerMetric: Timer =
    metric.timer(MetricInfo("surge.grpc.forward-command-timer", "The time taken by gRPC forwardCommand to forward the command to the aggregate"))

  override def forwardCommand(in: ForwardCommandRequest): Future[ForwardCommandReply] = forwardCommandTimerMetric.timeFuture {
    in.command match {
      case Some(cmd: protobuf.Command) =>
        logger.info(s"Received command for aggregate with id ${cmd.aggregateId}, payload has size ${cmd.payload.size()} (bytes)!")
        val aggIdStr = cmd.aggregateId
        Try(UUID.fromString(aggIdStr)) match {
          case Failure(exception) =>
            Future.failed(new Exception("Invalid aggregate id (not a UUID)!", exception))
          case Success(aggIdUUID) =>
            val surgeCmd: SurgeCmd = cmd
            logger.info(s"Forwarding command to aggregate with id ${cmd.aggregateId}!")
            surgeEngine.aggregateFor(aggIdUUID).sendCommand(surgeCmd).map {
              case CommandSuccess(aggregateState: Option[SurgeState]) =>
                val payloadSize: Int = aggregateState.map(_.payload.length).getOrElse(0)
                logger.info(s"Success! Aggregate state payload has size ${payloadSize} (bytes)")
                ForwardCommandReply(isSuccess = true, newState = aggregateState.map((item: SurgeState) => item: protobuf.State))
              case CommandFailure(reason) =>
                logger.error(reason, "Failure!")
                ForwardCommandReply(isSuccess = false, rejectionMessage = reason.getMessage, newState = None)
            }
        }
      case None =>
        // this should not happen
        // log warning message
        logger.warning("Requested to forward command but no command included!")
        Future.failed(new Exception("No command given to forward!"))
    }
  }

  private val getAggregateStateTimerMetric: Timer =
    metric.timer(MetricInfo("surge.grpc.get-aggregate-state-timer", "The time taken by gRPC getState to get the state of the aggregate"))

  override def getState(in: GetStateRequest): Future[GetStateReply] = getAggregateStateTimerMetric.timeFuture {
    logger.info(s"Business app asking for state of aggregate with id ${in.aggregateId}!")
    Try(UUID.fromString(in.aggregateId)) match {
      case Failure(exception) =>
        Future.failed(new Exception("Invalid aggregate id (not a UUID)!", exception))
      case Success(aggIdUUID) =>
        surgeEngine.aggregateFor(aggIdUUID).getState.map { maybeS: Option[SurgeState] =>
          {
            GetStateReply(in.aggregateId, state = maybeS.map(s => s: protobuf.State))
          }
        }
    }

  }

  override def healthCheck(in: HealthCheckRequest): Future[HealthCheckReply] =
    surgeEngine.healthCheck.map { healthCheck =>
      val status = Status.fromName(healthCheck.status.toUpperCase).getOrElse(Status.DOWN)
      HealthCheckReply(status = status)
    }

}
