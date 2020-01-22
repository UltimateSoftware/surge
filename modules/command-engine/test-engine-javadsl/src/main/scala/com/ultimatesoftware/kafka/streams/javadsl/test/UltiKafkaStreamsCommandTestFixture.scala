// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl.test

import com.ultimatesoftware.kafka.streams.javadsl.AggregateRef
import com.ultimatesoftware.mp.domain.{ EventMessageMetadataInfo, UltiAggregateCommandModel }
import com.ultimatesoftware.mp.serialization.message.Message
import com.ultimatesoftware.scala.core.domain.{ BasicStateTypeInfo, DefaultCommandMetadata, StateMessage }
import com.ultimatesoftware.scala.core.messaging.EventProperties
import com.ultimatesoftware.scala.core.utils.JsonFormats

import scala.annotation.varargs
import scala.compat.java8.FutureConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag

abstract class UltiKafkaStreamsCommandTestFixture[AggId, Agg, Cmd, Event, CmdMeta](
    ultiCommandModel: UltiAggregateCommandModel[AggId, Agg, Cmd, Event, CmdMeta]) {

  def extractEventAggregateId(event: Event): AggId

  val eventBus: EventBus[Message] = new EventBus[Message]
  val stateStore: StateStore[AggId, StateMessage[Agg]] = new StateStore[AggId, StateMessage[Agg]]

  def aggregateFor(aggregateId: AggId): AggregateRef[AggId, StateMessage[Agg], Cmd, CmdMeta] = {
    new TestAggregateRef[AggId, StateMessage[Agg], Cmd, Message, CmdMeta, EventProperties](
      aggregateId,
      ultiCommandModel, stateStore, eventBus)
  }

  @varargs def given(events: Event*): UltiKafkaStreamsCommandTestFixture[AggId, Agg, Cmd, Event, CmdMeta] = {
    events.foreach { event ⇒
      val aggId = extractEventAggregateId(event)
      val oldState = stateStore.getState(aggId)
      val evtProps = DefaultCommandMetadata.empty().toEventProperties

      val emptyMetaInfo = EventMessageMetadataInfo(None, None, None, None, None, None)
      val message = ultiCommandModel.createMessage(evtProps, event, ultiCommandModel.eventTypeInfo(event), emptyMetaInfo)

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
        val newStatePlusMeta = StateMessage.create[Agg](
          id = aggId.toString,
          aggregateId = Some(aggId.toString),
          tenantId = None,
          state = Some(state),
          eventSequenceNumber = newSequenceNumber,
          prevStateChecksum = oldState.flatMap(_.checksum),
          typeInfo = BasicStateTypeInfo(state.getClass.getName, "1.0.0"))(JsonFormats.jsonFormatterFromJackson(ClassTag(state.getClass)))

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
