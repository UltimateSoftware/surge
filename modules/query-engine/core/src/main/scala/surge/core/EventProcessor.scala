// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.{ AbstractProcessor, Processor, ProcessorContext }
import org.apache.kafka.streams.state.{ KeyValueStore, StoreBuilder, Stores }
import surge.metrics.MetricsProvider

class EventProcessor[Agg, Event](
    aggregateName: String,
    aggReads: SurgeAggregateReadFormatting[Agg],
    aggWrites: SurgeAggregateWriteFormatting[Agg],
    extractAggregateId: Event ⇒ Option[String],
    processEvent: (Option[Agg], Event) ⇒ Option[Agg],
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

  val supplier: () ⇒ Processor[String, Event] = {
    () ⇒ new StateProcessorImpl
  }

  private class StateProcessorImpl extends AbstractProcessor[String, Event] {
    private var keyValueStore: AggKTable = _

    override def init(context: ProcessorContext): Unit = {
      super.init(context)
      this.keyValueStore = context.getStateStore(aggregateKTableStoreName).asInstanceOf[AggKTable]
    }

    override def process(key: String, value: Event): Unit = {
      extractAggregateId(value).foreach { aggregateId ⇒
        val oldState = Option(keyValueStore.get(aggregateId))
        val previousBody = oldState
          .flatMap(state ⇒
            aggregateDeserializationTimer.time(aggReads.readState(state)))

        val newState = eventHandlingTimer.time(processEvent(previousBody, value))

        val newStateSerialized = newState.map { newStateValue ⇒
          aggregateSerializationTimer.time(aggWrites.writeState(newStateValue))
        }

        keyValueStore.put(aggregateId, newStateSerialized.map(_.value).orNull)
      }
    }

    override def close(): Unit = {
    }
  }
}
