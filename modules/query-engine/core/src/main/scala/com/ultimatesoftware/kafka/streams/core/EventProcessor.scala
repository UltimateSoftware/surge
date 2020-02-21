// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.scala.core.domain.{ StateMessage, StateTypeInfo }
import com.ultimatesoftware.scala.core.messaging.EventProperties
import com.ultimatesoftware.scala.oss.domain.AggregateSegment
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.{ AbstractProcessor, Processor, ProcessorContext }
import org.apache.kafka.streams.state.{ KeyValueStore, StoreBuilder, Stores }
import play.api.libs.json.{ Format, Json }

class EventProcessor[AggId, Agg, Event, EvtMeta <: EventProperties](
    aggregateTypeInfo: StateTypeInfo,
    aggReads: SurgeAggregateReadFormatting[AggId, StateMessage[Agg]],
    aggWrites: SurgeAggregateWriteFormatting[AggId, StateMessage[Agg]],
    processEvent: (Option[Agg], Event, EventProperties) ⇒ Option[Agg])(implicit aggFormat: Format[Agg]) {

  private val aggregateName = aggregateTypeInfo.fullTypeName
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
      value.meta.aggregateId.foreach { aggregateId ⇒
        val oldState = Option(keyValueStore.get(aggregateId.toString))
        val previousBody = oldState
          .flatMap(state ⇒ aggReads.readState(state))
          .flatMap(segment ⇒ segment.value.asOpt[StateMessage[Agg]])
          .flatMap(_.body)

        val newState = processEvent(previousBody, value.event, value.meta)

        // Wrap in state message -- This should be pulled out to the UltiLayer..
        val next: StateMessage[Agg] = StateMessage.create[Agg](
          aggregateId.toString,
          Some(aggregateId.toString),
          value.meta.tenantId,
          newState,
          value.meta.sequenceNumber,
          None, aggregateTypeInfo)

        val newStateSerialized = aggWrites.writeState(new AggregateSegment[AggId, StateMessage[Agg]](
          aggregateId.toString,
          Json.toJson(next), newState.map(c ⇒ c.getClass)))

        keyValueStore.put(aggregateId.toString, newStateSerialized)
      }

      // TODO this compiles but not sure if we want a context.commit() or context.forward() or something here
    }

    override def close(): Unit = {
    }
  }
}
