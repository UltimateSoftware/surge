// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.Done
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.Future

trait SurgeCommandServiceSink[AggId, Command, CmdMeta, Event, EvtMeta] extends EventSink[Event, EvtMeta] {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  def eventToCommand: Event ⇒ Option[Command]
  def surgeEngine: KafkaStreamsCommandTrait[AggId, _, Command, _, CmdMeta, _]
  protected def sendToAggregate(aggId: AggId, cmdMeta: CmdMeta, command: Command): Future[Any]

  def evtMetaToCmdMeta(evtMeta: EvtMeta): CmdMeta

  def handleEvent(event: Event, eventProps: EvtMeta): Future[Any] = {
    eventToCommand(event).map { command ⇒
      val aggId = surgeEngine.businessLogic.model.aggIdFromCommand(command)
      val cmdMeta = evtMetaToCmdMeta(eventProps)
      sendToAggregate(aggId, cmdMeta, command)
    } getOrElse {
      log.info(s"Skipping event with class ${event.getClass} since it was not converted into a command")
      Future.successful(Done)
    }
  }
}
