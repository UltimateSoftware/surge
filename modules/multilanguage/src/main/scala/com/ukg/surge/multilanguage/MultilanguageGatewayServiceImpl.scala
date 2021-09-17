// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.grpc.GrpcClientSettings
import com.typesafe.config.Config
import com.ukg.surge.multilanguage.protobuf._
import surge.scaladsl.command.SurgeCommand
import surge.scaladsl.common.{CommandFailure, CommandSuccess}

import java.net.InetAddress
import java.util.UUID
import scala.concurrent.Future
import scala.language.implicitConversions

class MultilanguageGatewayServiceImpl()(implicit system: ActorSystem) extends MultilanguageGatewayService {

  import Implicits._
  import system.dispatcher

  val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServiceImpl])

  val businessLogicgRPCClientConfig: Config = system.settings.config.getConfig("business-logic-server")
  val businessLogicgRPCPort: Int = businessLogicgRPCClientConfig.getInt("port")
  val kubernetesNamespace = "kafka"
  val podAddress = InetAddress.getLocalHost.getHostAddress.replace(".", "-")
  val podAddressCorrect = s"$podAddress.$kubernetesNamespace.pod.cluster.local"

  val businessLogicgRPCHost: String = podAddressCorrect

  logger.info(s"Business logic gRPC host and port: ${businessLogicgRPCHost}:${businessLogicgRPCPort}")

  val businessLogicgRPCClientSettings: GrpcClientSettings = GrpcClientSettings.connectToServiceAt(businessLogicgRPCHost, businessLogicgRPCPort).withTls(false)

  val bridgeToBusinessApp: BusinessLogicService = BusinessLogicServiceClient(businessLogicgRPCClientSettings)

  val genericSurgeCommandBusinessLogic = new GenericSurgeCommandBusinessLogic(bridgeToBusinessApp)

  lazy val surgeEngine: SurgeCommand[UUID, SurgeState, SurgeCmd, Nothing, SurgeEvent] = {
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
        val aggIdUUID: UUID = UUID.fromString(aggIdStr)
        val surgeCmd: SurgeCmd = cmd
        logger.info(s"Forwarding command to aggregate with id ${cmd.aggregateId}!")
        surgeEngine.aggregateFor(aggIdUUID).sendCommand(surgeCmd).map {
          case CommandSuccess(aggregateState: Option[SurgeState]) =>
            val payloadSize: Any = aggregateState.map(_.payload.length).getOrElse(0)
            logger.info(s"Success! Aggregate state payload has size ${payloadSize} (bytes)")
            ForwardCommandReply(isSuccess = true, newState = aggregateState.map((item: SurgeState) => item: protobuf.State))
          case CommandFailure(reason) =>
            logger.error(reason, "Failure!")
            ForwardCommandReply(isSuccess = false, rejectionMessage = reason.getMessage, newState = None)
        }
      case None =>
        // this should not happen
        // log warning message
        logger.warning("Requested to forward command but no command included!")
        Future.successful(ForwardCommandReply(isSuccess = false, rejectionMessage = "No command given to forward", newState = None))
    }
  }
}
