// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.grpc.GrpcClientSettings
import com.ukg.surge.multilanguage.protobuf._
import com.typesafe.config.Config
import surge.scaladsl.command.SurgeCommand
import surge.scaladsl.common.{ CommandFailure, CommandSuccess }

import java.util.UUID
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

class MultilanguageGatewayServiceImpl(bridgeToBusinessApp: BusinessLogicService, aggregateName: String, eventsTopicName: String, stateTopicName: String)(
    implicit system: ActorSystem)
    extends MultilanguageGatewayService {

  import Implicits._
  import system.dispatcher

  val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServiceImpl])

  val genericSurgeCommandBusinessLogic = new GenericSurgeCommandBusinessLogic(aggregateName, eventsTopicName, stateTopicName, bridgeToBusinessApp)

  val surgeEngine: SurgeCommand[UUID, SurgeState, SurgeCmd, Nothing, SurgeEvent] = {
    val engine = SurgeCommand(system, genericSurgeCommandBusinessLogic, system.settings.config)
    engine.start()
    logger.info("Started engine!")
    engine
  }

  override def forwardCommand(in: ForwardCommandRequest): Future[ForwardCommandReply] = {
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

  override def getState(in: GetStateRequest): Future[GetStateReply] = {
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

}
