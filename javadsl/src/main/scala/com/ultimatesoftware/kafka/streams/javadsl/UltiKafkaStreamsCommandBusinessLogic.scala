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
  extends KafkaStreamsCommandBusinessLogic[AggId, StateMessage[Agg], Cmd, Message, CmdMeta, EventProperties] {
  override def eventKeyExtractor(evtMsg: Message): String = s"${evtMsg.getAggregateId}:${evtMsg.getSequenceNumber}"

  override def stateKeyExtractor(jsValue: JsValue): String = {
    jsValue.asOpt[StateMessage[JsValue]].map(_.fullIdentifier).getOrElse("")
  }

  def aggregateClass: Class[Agg]

  // this seems wrong
  override def aggregateTargetClass: Class[StateMessage[Agg]] = classOf[StateMessage[Agg]]

  // FIXME this should be handled more similarly to the way the envelope is
  private def jacksonWriteFormatter = new JacksonWriteFormatter[AggId, StateMessage[Agg], Event, EventProperties]()
  def stateWriteFormatting: SurgeAggregateWriteFormatting[AggId, StateMessage[Agg]] = (agg: AggregateSegment[AggId, StateMessage[Agg]]) ⇒ {
    jacksonWriteFormatter.writeState(agg)
  }

  override def commandValidator: AsyncCommandValidator[Cmd, StateMessage[Agg]] = AsyncCommandValidator { _ ⇒ Seq() }

}
