// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import com.ultimatesoftware.mp.serialization.message.Message
import com.ultimatesoftware.scala.core.domain.StateMessage
import com.ultimatesoftware.scala.core.messaging.EventProperties
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import play.api.libs.json.JsValue

abstract class UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta]
  extends KafkaStreamsCommandBusinessLogic[AggId, StateMessage[Agg], Cmd, Message, CmdMeta, EventProperties] {
  override def eventKeyExtractor(evtMsg: Message): String = s"${evtMsg.getAggregateId}:${evtMsg.getSequenceNumber}"

  override def stateKeyExtractor(jsValue: JsValue): String = {
    jsValue.asOpt[StateMessage[JsValue]].map(_.fullIdentifier).getOrElse("")
  }

  override def commandValidator: AsyncCommandValidator[Cmd, StateMessage[Agg]] = AsyncCommandValidator { _ ⇒ Seq() }
}
