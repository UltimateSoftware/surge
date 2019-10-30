// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl.test

import akka.actor.ActorSystem
import com.ultimatesoftware.kafka.streams.core.KafkaStreamsCommandBusinessLogic
import com.ultimatesoftware.kafka.streams.javadsl.{ AggregateRef, UltiKafkaStreamsCommand, UltiKafkaStreamsCommandBusinessLogic }
import com.ultimatesoftware.scala.core.domain.StatePlusMetadata
import com.ultimatesoftware.scala.core.messaging.{ EventMessage, EventProperties }

class EmbeddedUltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta](
    testName: String,
    val ultiBusinessLogic: UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta])
  extends UltiKafkaStreamsCommand[AggId, Agg, Cmd, Event, CmdMeta] {

  override val actorSystem: ActorSystem = ActorSystem(testName)

  override val businessLogic: KafkaStreamsCommandBusinessLogic[AggId, StatePlusMetadata[Agg], Cmd, EventMessage[Event], CmdMeta, EventProperties] =
    ultiBusinessLogic.toCore

  val eventBus: EventBus[EventMessage[Event]] = new EventBus[EventMessage[Event]]
  val stateStore: StateStore[AggId, StatePlusMetadata[Agg]] = new StateStore[AggId, StatePlusMetadata[Agg]]

  override def aggregateFor(aggregateId: AggId): AggregateRef[AggId, StatePlusMetadata[Agg], Cmd, CmdMeta] = {
    new TestAggregateRef[AggId, StatePlusMetadata[Agg], Cmd, EventMessage[Event], CmdMeta, EventProperties](
      aggregateId,
      ultiBusinessLogic, stateStore, eventBus)
  }

  override def start(): Unit = {}
}
