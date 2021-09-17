// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import com.ukg.surge.multilanguage
import com.ukg.surge.multilanguage.protobuf.{ BusinessLogicService, HandleEventRequest, HandleEventResponse, ProcessCommandRequest }
import surge.scaladsl.command.AggregateCommandModel

import scala.concurrent.Await
import scala.util.{ Failure, Success, Try }
import scala.concurrent.duration._

class GenericAggregateCommandModel(bridgeToBusinessApp: BusinessLogicService)(implicit system: ActorSystem)
    extends AggregateCommandModel[SurgeState, SurgeCmd, SurgeEvent] {

  import Implicits._
  val logger: LoggingAdapter = Logging(system, classOf[GenericAggregateCommandModel])

  override def processCommand(aggregate: Option[SurgeState], surgeCommand: SurgeCmd): Try[Seq[SurgeEvent]] = {
    logger.info(s"Processing command (aggregate id is ${surgeCommand.aggregateId}). Aggregate is defined: ${aggregate.isDefined}.")

    val maybePbState: Option[protobuf.State] = aggregate.map(surgeState => surgeState: protobuf.State)
    val pbCommand: protobuf.Command = surgeCommand: multilanguage.protobuf.Command
    val processCommandRequest = ProcessCommandRequest(maybePbState, Some(pbCommand))

    try {
      logger.info("Making gRPC call to business app!")
      val call = bridgeToBusinessApp.processCommand(processCommandRequest)
      logger.info("Called business app via gRPC!")
      val reply = Await.result(call, atMost = 20.seconds)
      if (reply.rejection == "") {
        logger.info("Got response from business app!")
        Success(reply.events.map(pbEvent => pbEvent: SurgeEvent))
      } else {
        Failure(new Exception(reply.rejection))
      }
    } catch {
      case e: Exception =>
        logger.error(e, "Error making gRPC call to business app from processCommand")
        throw e
    }
  }

  override def handleEvent(aggregate: Option[SurgeState], surgeEvent: SurgeEvent): Option[SurgeState] = {
    logger.info("Handling event!")
    val maybePbState: Option[protobuf.State] = aggregate.map(surgeState => surgeState: protobuf.State)
    val pbEvent = surgeEvent
    val handleEventRequest = HandleEventRequest(maybePbState, Some(pbEvent))
    val call = bridgeToBusinessApp.handleEvent(handleEventRequest)
    logger.info("Called business app via gRPC!")
    val reply: HandleEventResponse = Await.result(call, atMost = 7.seconds)
    reply.state.map(pbState => pbState: SurgeState)
  }
}
