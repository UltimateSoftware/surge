// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional

import com.ultimatesoftware.kafka.streams.core.{ SurgeReadFormatting, SurgeWriteFormatting }
import com.ultimatesoftware.scala.core.domain.{ StateMessage, StateTypeInfo }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.messaging.EventProperties

case class KafkaStreamsQueryBusinessLogic[AggId, Agg, Event, EvtMeta <: EventProperties](
    aggregateName: String,
    stateTopic: KafkaTopic,
    eventsTopic: KafkaTopic,
    readFormatting: SurgeReadFormatting[AggId, StateMessage[Agg], Event, EvtMeta],
    writeFormatting: SurgeWriteFormatting[AggId, StateMessage[Agg], Event, EvtMeta],
    aggIdExtractor: AggIdExtractor[AggId, Agg],
    eventProcessor: EventProcessor[Agg, Event],
    aggregateTypeInfo: StateTypeInfo)

trait EventProcessor[Agg, Event] {
  def handleEvent(currentAggregate: Optional[Agg], event: Event, eventProps: EventProperties): Optional[Agg]
}

trait AggIdExtractor[AggId, Agg] {
  def extractAggId(agg: Agg): AggId
}
