// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.scala.core.monitoring.metrics.MetricsProvider
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.{ AbstractProcessor, Processor, ProcessorContext }
import org.apache.kafka.streams.state.{ KeyValueStore, StoreBuilder, Stores }

class EventProcessor[AggId, Agg, Event, EvtMeta](
    aggregateName: String,
    aggReads: SurgeAggregateReadFormatting[AggId, Agg],
    aggWrites: SurgeAggregateWriteFormatting[AggId, Agg],
    extractAggregateId: EventPlusMeta[Event, EvtMeta] ⇒ Option[String],
    processEvent: (Option[Agg], Event, EvtMeta) ⇒ Option[Agg],
    metricsProvider: MetricsProvider) {

  val aggregateDeserializationTimer = metricsProvider.createTimer(s"${aggregateName}AggregateStateDeserializationTimer")
  val aggregateSerializationTimer = metricsProvider.createTimer(s"${aggregateName}AggregateStateSerializationTimer")
  val eventHandlingTimer = metricsProvider.createTimer(s"${aggregateName}EventHandlingTimer")

  val aggregateKTableStoreName: String = s"aggregate-state.$aggregateName"
  type AggKTable = KeyValueStore[String, Array[Byte]]

  val aggregateKTableStoreBuilder: StoreBuilder[AggKTable] = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore(aggregateKTableStoreName),
    Serdes.String(),
    Serdes.ByteArray())

  val supplier: () ⇒ Processor[String, EventPlusMeta[Event, EvtMeta]] = {
    () ⇒ new StateProcessorImpl
  }

  private class StateProcessorImpl extends AbstractProcessor[String, EventPlusMeta[Event, EvtMeta]] {
    private var keyValueStore: AggKTable = _

    override def init(context: ProcessorContext): Unit = {
      super.init(context)
      this.keyValueStore = context.getStateStore(aggregateKTableStoreName).asInstanceOf[AggKTable]
    }

    override def process(key: String, value: EventPlusMeta[Event, EvtMeta]): Unit = {
      extractAggregateId(value).foreach { aggregateId ⇒
        val oldState = Option(keyValueStore.get(aggregateId))
        val previousBody = oldState
          .flatMap(state ⇒
            aggregateDeserializationTimer.time(aggReads.readState(state)))

        val newState = eventHandlingTimer.time(processEvent(previousBody, value.event, value.meta))

        val newStateSerialized = newState.map { newStateValue ⇒
          aggregateSerializationTimer.time(aggWrites.writeState(newStateValue))
        }

        keyValueStore.put(aggregateId, newStateSerialized.orNull)
      }
    }

    override def close(): Unit = {
    }
  }
}
