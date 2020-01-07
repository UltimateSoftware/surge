// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import com.ultimatesoftware.kafka.streams.core.SurgeAggregateWriteFormatting
import com.ultimatesoftware.mp.serialization.message.Message
import com.ultimatesoftware.scala.core.domain.{ StateMessage, StatePlusMetadata }
import com.ultimatesoftware.scala.core.messaging.EventProperties
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import com.ultimatesoftware.scala.oss.domain.AggregateSegment
import play.api.libs.json.JsValue

abstract class UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta]
  extends KafkaStreamsCommandBusinessLogic[AggId, StatePlusMetadata[Agg], Cmd, Message, CmdMeta, EventProperties] {
  override def eventKeyExtractor(evtMsg: Message): String = s"${evtMsg.getAggregateId}:${evtMsg.getSequenceNumber}"

  override def stateKeyExtractor(jsValue: JsValue): String = {
    jsValue.asOpt[StateMessage[JsValue]].map(_.fullIdentifier).getOrElse("")
  }

  def aggregateClass: Class[Agg]

  override def aggregateTargetClass: Class[StatePlusMetadata[Agg]] = classOf[StatePlusMetadata[Agg]]

  // FIXME this should be handled more similarly to the way the envelope is
  private def jacksonWriteFormatter = new JacksonWriteFormatter[AggId, StatePlusMetadata[Agg], Event, EventProperties]()
  def stateWriteFormatting: SurgeAggregateWriteFormatting[AggId, StatePlusMetadata[Agg]] = new SurgeAggregateWriteFormatting[AggId, StatePlusMetadata[Agg]] {
    override def writeState(agg: AggregateSegment[AggId, StatePlusMetadata[Agg]]): Array[Byte] = {
      jacksonWriteFormatter.writeState(agg)
    }
  }

  override def commandValidator: AsyncCommandValidator[Cmd, StatePlusMetadata[Agg]] = AsyncCommandValidator { _ ⇒ Seq() }

}
