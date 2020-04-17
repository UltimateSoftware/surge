// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.{ ExecutionContext, Future }

trait SurgeMultiCommandServiceSink[AggId, Command, CmdMeta, Event, EvtMeta] extends EventSink[Event, EvtMeta] {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  def eventToCommands: Event ⇒ Seq[Command]
  def surgeEngine: KafkaStreamsCommandTrait[AggId, _, Command, _, CmdMeta, _]
  implicit def executionContext: ExecutionContext
  protected def sendToAggregate(aggId: AggId, cmdMeta: CmdMeta, command: Command): Future[Any]

  def evtMetaToCmdMeta(evtMeta: EvtMeta): CmdMeta

  def handleEvent(event: Event, eventProps: EvtMeta): Future[Any] = {
    val commands = eventToCommands(event)
    if (commands.isEmpty) {
      log.info(s"Skipping event with class ${event.getClass} since it was not converted into a command")
    }
    val appliedCommands = commands.map { command ⇒
      val aggId = surgeEngine.businessLogic.model.aggIdFromCommand(command)
      val cmdMeta = evtMetaToCmdMeta(eventProps)
      sendToAggregate(aggId, cmdMeta, command)
    }
    Future.sequence(appliedCommands)
  }
}

trait SurgeCommandServiceSink[AggId, Command, CmdMeta, Event, EvtMeta]
  extends SurgeMultiCommandServiceSink[AggId, Command, CmdMeta, Event, EvtMeta] {
  def eventToCommand: Event ⇒ Option[Command]
  override def eventToCommands: Event ⇒ Seq[Command] = eventToCommand.andThen(_.toSeq)
}
