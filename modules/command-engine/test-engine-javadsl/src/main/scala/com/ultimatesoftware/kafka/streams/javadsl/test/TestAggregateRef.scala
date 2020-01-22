// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl.test

import java.util.Optional
import java.util.concurrent.CompletionStage

import com.ultimatesoftware.kafka.streams.javadsl.AggregateRef
import com.ultimatesoftware.scala.oss.domain.AggregateCommandModel

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future
import scala.util.{ Failure, Success }

case class TestAggregateRef[AggId, Agg, Cmd, Event, CmdMeta, EvtMeta](
    aggregateId: AggId,
    commandModel: AggregateCommandModel[AggId, Agg, Cmd, Event, CmdMeta, EvtMeta],
    stateStore: StateStore[AggId, Agg],
    eventBus: EventBus[Event]) extends AggregateRef[AggId, Agg, Cmd, CmdMeta] {

  override def getState: CompletionStage[Optional[Agg]] = {
    Future.successful(stateStore.getState(aggregateId).asJava).toJava
  }

  override def ask(commandProps: CmdMeta, command: Cmd): CompletionStage[Optional[Agg]] = {
    val currentState = stateStore.getState(aggregateId)

    commandModel.processCommand(currentState, command, commandProps) match {
      case Success(events) ⇒
        eventBus.send(events)
        val evtMeta = commandModel.cmdMetaToEvtMeta(commandProps)
        val newState = events.foldLeft(currentState) {
          (stateAccum, event) ⇒ commandModel.handleEvent(stateAccum, event, evtMeta)
        }
        stateStore.putState(aggregateId, newState)
        Future.successful(newState.asJava).toJava
      case Failure(exception) ⇒
        Future.failed(exception).toJava
    }
  }
}
