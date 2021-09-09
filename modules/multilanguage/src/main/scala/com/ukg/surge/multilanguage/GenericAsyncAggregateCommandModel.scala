// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import com.ukg.surge.multilanguage
import com.ukg.surge.multilanguage.protobuf._
import surge.scaladsl.command.AsyncAggregateCommandModel

import scala.concurrent.{ ExecutionContext, Future }

class GenericAsyncAggregateCommandModel(bridgeToBusinessApp: BusinessLogicService)(implicit system: ActorSystem)
    extends AsyncAggregateCommandModel[SurgeState, SurgeCmd, SurgeEvent] {

  import Implicits._
  val logger: LoggingAdapter = Logging(system, classOf[GenericAsyncAggregateCommandModel])
  import system.dispatcher

  override def executionContext: ExecutionContext = system.dispatcher

  override def processCommand(aggregate: Option[SurgeState], surgeCommand: SurgeCmd): Future[Seq[SurgeEvent]] = {
    logger.info(s"""Calling command handler of business app via gRPC.
         |Aggregate id: ${surgeCommand.aggregateId}).
         |State defined: ${aggregate.isDefined}.
         |Command payload size: ${surgeCommand.payload.length} (bytes).""".stripMargin)

    val maybePbState: Option[protobuf.State] = aggregate.map(surgeState => surgeState: protobuf.State)
    val pbCommand: protobuf.Command = surgeCommand: multilanguage.protobuf.Command
    val processCommandRequest = ProcessCommandRequest(maybePbState, Some(pbCommand))
    val reply: Future[ProcessCommandReply] = bridgeToBusinessApp.processCommand(processCommandRequest)

    reply.flatMap { processCommandReply =>
      {
        if (processCommandReply.isSuccess) {
          Future.successful {
            processCommandReply.events.map(e => e: SurgeEvent)
          }
        } else {
          Future.failed(new Exception(processCommandReply.rejectionMessage))
        }
      }
    }
  }

  override def handleEvents(aggregate: Option[SurgeState], surgeEvents: Seq[SurgeEvent]): Future[Option[SurgeState]] = {
    logger.info(
      s"""Calling event handler of business app via gRPC.
         |State defined: ${aggregate.isDefined}.
         |Num events: ${surgeEvents.size}.
         |Event payload sizes (bytes): $${surgeEvents.map(_.payload.length).mkString(""".stripMargin,
      ")}.")
    val maybePbState: Option[protobuf.State] = aggregate.map(surgeState => surgeState: protobuf.State)
    val handleEventRequest = HandleEventsRequest(maybePbState, surgeEvents.map(surgeEvent => surgeEvent: protobuf.Event))
    val reply: Future[HandleEventsResponse] = bridgeToBusinessApp.handleEvents(handleEventRequest)
    reply.map { r => r.state.map(state => state: SurgeState) }
  }
}
