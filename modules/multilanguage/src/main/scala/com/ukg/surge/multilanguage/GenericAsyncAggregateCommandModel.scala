// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import com.ukg.surge.multilanguage
import com.ukg.surge.multilanguage.protobuf._
import surge.metrics.{ MetricInfo, Metrics, Timer }
import surge.scaladsl.command.AsyncAggregateCommandModel

import scala.concurrent.{ ExecutionContext, Future }

class GenericAsyncAggregateCommandModel(bridgeToBusinessApp: BusinessLogicService)(implicit system: ActorSystem)
    extends AsyncAggregateCommandModel[SurgeState, SurgeCmd, SurgeEvent] {

  import Implicits._
  val logger: LoggingAdapter = Logging(system, classOf[GenericAsyncAggregateCommandModel])
  import system.dispatcher

  override def executionContext: ExecutionContext = system.dispatcher

  /**
   * sanity check: identifiers should match
   */
  def validIds(aggregate: Option[SurgeState], surgeCmd: SurgeCmd): Boolean = {
    aggregate.map(_.aggregateId) match {
      case Some(value: String) => surgeCmd.aggregateId == value
      case None                => true
    }
  }

  /**
   * sanity check: identifiers should match
   */
  def validIds(aggregate: Option[SurgeState], surgeEvents: Seq[SurgeEvent]): Boolean = {
    val aggsIds = surgeEvents.map(_.aggregateId).distinct
    aggregate.map(_.aggregateId) match {
      case Some(value) =>
        aggsIds.size == 1 && aggsIds.head == value
      case None =>
        aggsIds.size == 1
    }
  }

  private val metric: Metrics = Metrics.globalMetricRegistry
  private val processCommandTimerMetric: Timer =
    metric.timer(Metrics.SURGE_GRPC_PROCESS_COMMAND_TIMER)

  override def processCommand(aggregate: Option[SurgeState], surgeCommand: SurgeCmd): Future[Seq[SurgeEvent]] =
    processCommandTimerMetric.timeFuture {
      if (!validIds(aggregate, surgeCommand)) {
        Future.failed(new Exception("Wrong aggregate ids!"))
      } else {
        logger.info(
          s"Calling command handler of business app via gRPC. Aggregate id: ${surgeCommand.aggregateId})." +
            s" State defined: ${aggregate.isDefined}. Command payload size: ${surgeCommand.payload.length} (bytes).")

        val maybePbState: Option[protobuf.State] = aggregate.map(surgeState => surgeState: protobuf.State)
        val pbCommand: protobuf.Command = surgeCommand: multilanguage.protobuf.Command
        val processCommandRequest = ProcessCommandRequest(aggregateId = surgeCommand.aggregateId, maybePbState, Some(pbCommand))
        val reply: Future[ProcessCommandReply] = bridgeToBusinessApp.processCommand(processCommandRequest)

        reply.flatMap { processCommandReply =>
          {
            if (processCommandReply.isSuccess) {
              logger.info(s"""Called command handler of business app via gRPC. Got back ${processCommandReply.events.size} events!""")
              Future.successful {
                processCommandReply.events.map(e => e: SurgeEvent)
              }
            } else {
              logger.info(s"""Called command handler of business app via gRPC. Got a rejection message: ${processCommandReply.rejectionMessage}!""")
              Future.failed(new Exception(processCommandReply.rejectionMessage))
            }
          }
        }
      }
    }

  private val handleEventsTimerMetric: Timer =
    metric.timer(Metrics.SURGE_GRPC_HANDLE_EVENTS_TIMER)

  override def handleEvents(aggregate: Option[SurgeState], surgeEvents: Seq[SurgeEvent]): Future[Option[SurgeState]] =
    handleEventsTimerMetric.timeFuture {
      if (surgeEvents.isEmpty) {
        logger.warning("handleEvents called but no events provided!")
        Future.successful(aggregate)
      } else {
        if (!validIds(aggregate, surgeEvents)) {
          Future.failed(new Exception("handleEvents called but wrong aggregate ids!"))
        } else {
          val aggregateId = aggregate.map(_.aggregateId).orElse(surgeEvents.headOption.map(_.aggregateId)).get
          logger.info(
            s"Calling event handler of business app via gRPC. Aggregate id: $aggregateId." +
              s" State defined: ${aggregate.isDefined}." + s" Num events: ${surgeEvents.size}. " +
              s"Event payload sizes (bytes): ${surgeEvents.map(_.payload.length).mkString(",")}.")
          val maybePbState: Option[protobuf.State] = aggregate.map(surgeState => surgeState: protobuf.State)
          val handleEventRequest = HandleEventsRequest(aggregateId, maybePbState, surgeEvents.map(surgeEvent => surgeEvent: protobuf.Event))
          val reply: Future[HandleEventsResponse] = bridgeToBusinessApp.handleEvents(handleEventRequest)
          reply.map { r => r.state.map(state => state: SurgeState) }
        }
      }
    }
}
