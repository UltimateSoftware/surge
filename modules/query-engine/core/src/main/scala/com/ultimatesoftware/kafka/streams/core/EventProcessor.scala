// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.scala.oss.domain.AggregateSegment
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.{ AbstractProcessor, Processor, ProcessorContext }
import org.apache.kafka.streams.state.{ KeyValueStore, StoreBuilder, Stores }
import play.api.libs.json.{ Format, Json }

class EventProcessor[AggId, Agg, Event, EvtMeta](
    aggregateName: String,
    aggReads: SurgeAggregateReadFormatting[AggId, Agg],
    aggWrites: SurgeAggregateWriteFormatting[AggId, Agg],
    extractAggregateId: EventPlusMeta[Event, EvtMeta] ⇒ Option[String],
    processEvent: (Option[Agg], Event, EvtMeta) ⇒ Option[Agg])(implicit aggFormat: Format[Agg]) {

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
        val oldState = Option(keyValueStore.get(aggregateId.toString))
        val previousBody = oldState
          .flatMap(state ⇒ aggReads.readState(state))
          .flatMap(segment ⇒ segment.value.asOpt[Agg])

        val newState = processEvent(previousBody, value.event, value.meta)

        val newStateSerialized = aggWrites.writeState(new AggregateSegment[AggId, Agg](
          aggregateId.toString,
          Json.toJson(newState), newState.map(c ⇒ c.getClass)))

        keyValueStore.put(aggregateId.toString, newStateSerialized)
      }
    }

    override def close(): Unit = {
    }
  }
}
