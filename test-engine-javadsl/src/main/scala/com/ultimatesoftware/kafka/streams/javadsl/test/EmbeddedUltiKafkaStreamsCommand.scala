// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl.test

import java.util.concurrent.{ Callable, TimeUnit }

import com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic
import com.ultimatesoftware.kafka.streams.javadsl.{ AggregateRef, UltiKafkaStreamsCommand, UltiKafkaStreamsCommandBusinessLogic }
import com.ultimatesoftware.scala.core.domain.StatePlusMetadata
import com.ultimatesoftware.scala.core.messaging.{ EventMessage, EventProperties }
import org.awaitility.Awaitility._
import org.awaitility.core.ThrowingRunnable

class EmbeddedUltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta](
    val ultiBusinessLogic: UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta])
  extends UltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta] {

  override val businessLogic: KafkaStreamsCommandBusinessLogic[AggId, StatePlusMetadata[Agg], Cmd, EventMessage[Event], CmdMeta, EventProperties] =
    ultiBusinessLogic.toCore

  val eventBus: EventBus[EventMessage[Event]] = new EventBus[EventMessage[Event]]
  val stateStore: StateStore[AggId, StatePlusMetadata[Agg]] = new StateStore[AggId, StatePlusMetadata[Agg]]

  override def aggregateFor(aggregateId: AggId): AggregateRef[AggId, StatePlusMetadata[Agg], Cmd, CmdMeta] = {
    new TestAggregateRef[AggId, StatePlusMetadata[Agg], Cmd, EventMessage[Event], CmdMeta, EventProperties](
      aggregateId,
      ultiBusinessLogic.commandModel, stateStore, eventBus)
  }

  override def start(): Unit = {}
}

class EmbeddedEngineTestHelper[AggId, Agg, Cmd, Event, CmdMeta](engine: EmbeddedUltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta]) {
  private val timeoutSeconds = 30
  val eventBus: EventBus[EventMessage[Event]] = engine.eventBus
  val stateStore: StateStore[AggId, StatePlusMetadata[Agg]] = engine.stateStore

  def clearReceivedEvents(): Unit = {
    eventBus.clear()
  }

  private implicit def byNameFunctionToCallableOfType[T](function: ⇒ T): Callable[T] = () ⇒ function
  private implicit def byNameFunctionToCallableOfBoolean(function: ⇒ scala.Boolean): Callable[java.lang.Boolean] = () ⇒ function
  private implicit def byNameFunctionToRunnable[T](function: ⇒ T): ThrowingRunnable = () ⇒ function

  def waitForEvent[T](`type`: Class[T]): T = {
    var event: Option[T] = None
    await().atMost(timeoutSeconds, TimeUnit.SECONDS) until {
      eventBus.consumeOne().map(_.body) match {
        case Some(evt) if evt.getClass == `type` ⇒
          event = Some(evt.asInstanceOf[T])
        case _ ⇒
        // Do nothing
      }
      event.nonEmpty
    }

    event.getOrElse(throw new SurgeAssertionError(s"Event of type ${`type`} not found in the eventbus"))
  }

}
