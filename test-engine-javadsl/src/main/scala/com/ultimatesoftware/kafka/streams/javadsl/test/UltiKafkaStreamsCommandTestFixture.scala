// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl.test

import com.ultimatesoftware.kafka.streams.javadsl.AggregateRef
import com.ultimatesoftware.mp.domain.UltiAggregateCommandModel
import com.ultimatesoftware.mp.serialization.message.Message
import com.ultimatesoftware.scala.core.domain.{ DefaultCommandMetadata, StatePlusMetadata }
import com.ultimatesoftware.scala.core.messaging.EventProperties

import scala.annotation.varargs
import scala.compat.java8.FutureConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

abstract class UltiKafkaStreamsCommandTestFixture[AggId, Agg, Cmd, Event, CmdMeta](
    ultiCommandModel: UltiAggregateCommandModel[AggId, Agg, Cmd, Event, CmdMeta]) {

  def extractEventAggregateId(event: Event): AggId

  val eventBus: EventBus[Message] = new EventBus[Message]
  val stateStore: StateStore[AggId, StatePlusMetadata[Agg]] = new StateStore[AggId, StatePlusMetadata[Agg]]

  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, StatePlusMetadata[Agg], Cmd, CmdMeta] = {
    new TestAggregateRef[AggId, StatePlusMetadata[Agg], Cmd, Message, CmdMeta, EventProperties](
      aggregateId,
      ultiCommandModel, stateStore, eventBus)
  }

  @varargs def given(events: Event*): UltiKafkaStreamsCommandTestFixture[AggId, Agg, Cmd, Event, CmdMeta] = {
    events.foreach { event ⇒
      val aggId = extractEventAggregateId(event)
      val oldState = stateStore.getState(aggId)
      val evtProps = DefaultCommandMetadata.empty().toEventProperties

      val message = ultiCommandModel.createMessage(evtProps, event, ultiCommandModel.eventTypeInfo(event))

      val newState = ultiCommandModel.handleEvent(oldState, message, evtProps)

      stateStore.putState(aggId, newState)
      eventBus.send(message)
      eventBus.consumeOne() // Since the played event is given, don't expect to consume it from the event bus
    }
    this
  }

  def givenStates(states: Map[AggId, Agg]): UltiKafkaStreamsCommandTestFixture[AggId, Agg, Cmd, Event, CmdMeta] = {
    states.foreach {
      case (aggId, state) ⇒
        val oldState = stateStore.getState(aggId)
        val newSequenceNumber = oldState.map(_.eventSequenceNumber).getOrElse(1) + 1
        val newStatePlusMeta = StatePlusMetadata[Agg](state = Some(state), eventSequenceNumber = newSequenceNumber,
          tenantId = None, checksums = oldState.map(_.checksums).getOrElse(Map.empty))

        stateStore.putState(aggId, newStatePlusMeta)
    }
    this
  }

  def when(command: Cmd, meta: CmdMeta): UltiKafkaStreamsCommandTestFixture[AggId, Agg, Cmd, Event, CmdMeta] = {
    val aggregateId = ultiCommandModel.aggIdFromCommand(command)
    Await.result(aggregateFor(aggregateId).ask(meta, command).toScala, 10.seconds)

    this
  }

  @varargs def expectEvents(events: Event*): UltiKafkaStreamsCommandTestFixture[AggId, Agg, Cmd, Event, CmdMeta] = {
    events.foreach { expectedEvent ⇒
      val actualEventOpt = eventBus.consumeOne().map(_.getPayload.asInstanceOf[Event])
      val actualMatchesExpected = actualEventOpt.exists(actual ⇒ verifyEventEquality(expectedEvent, actual))
      if (!actualMatchesExpected) {
        val actualEventString = actualEventOpt.map(_.toString).getOrElse("none")
        val errorDetails =
          s"""The published events do not match the expected events.  Expected:
             |$expectedEvent
             |But got:
             |$actualEventString
             |""".stripMargin

        throw new SurgeAssertionError(errorDetails)
      }
    }
    this
  }

  private def verifyEventEquality(expectedEvent: Event, actualEvent: Event): Boolean = {
    if (expectedEvent.getClass != actualEvent.getClass) {
      false
    } else {
      val matcher = new EqualFieldsMatcher[Event](expectedEvent)
      matcher.matches(actualEvent)
    }
  }

  def expectNoEvents(): UltiKafkaStreamsCommandTestFixture[AggId, Agg, Cmd, Event, CmdMeta] = {
    val actualEvent = eventBus.consumeOne()
    if (actualEvent.nonEmpty) {
      throw new SurgeAssertionError(s"The published events do not match the expected events.  Expected none, but got:\n${actualEvent.getOrElse("")}")
    }
    this
  }
}
