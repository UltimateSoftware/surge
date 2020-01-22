// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import com.ultimatesoftware.kafka.streams.core.{ SurgeReadFormatting, SurgeWriteFormatting }
import com.ultimatesoftware.scala.core.domain._
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.messaging.EventProperties

case class KafkaStreamsQueryBusinessLogic[AggId, Agg, Event, EvtMeta <: EventProperties](
    aggregateName: String,
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    extractAggId: Agg ⇒ AggId,
    readFormatting: SurgeReadFormatting[AggId, StateMessage[Agg], Event, EvtMeta],
    writeFormatting: SurgeWriteFormatting[AggId, StateMessage[Agg], Event, EvtMeta],
    eventProcessor: EventProcessor[Agg, Event],
    aggregateTypeInfo: StateTypeInfo)

trait EventProcessor[Agg, Event] {
  def handleEvent(currentAggregate: Option[Agg], event: Event, eventProps: EventProperties): Option[Agg]
}
