// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import com.ultimatesoftware.mp.serialization.message.Message
import com.ultimatesoftware.scala.core.domain.StateMessage
import com.ultimatesoftware.scala.core.messaging.EventProperties
import com.ultimatesoftware.scala.core.validations.{ AsyncCommandValidator, MessagePlusCurrentAggregate, ValidationDSL }
import play.api.libs.json.JsValue

trait UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta]
  extends KafkaStreamsCommandBusinessLogic[AggId, StateMessage[Agg], Cmd, Message, CmdMeta, EventProperties] {

  override def eventKeyExtractor: Message ⇒ String = { evtMsg ⇒ s"${evtMsg.getAggregateId}:${evtMsg.getSequenceNumber}" }

  override def stateKeyExtractor: JsValue ⇒ String = { jsValue ⇒
    jsValue.asOpt[StateMessage[JsValue]].map(_.fullIdentifier).getOrElse("")
  }

  private def convertStatePlusMeta: MessagePlusCurrentAggregate[Cmd, StateMessage[Agg]] ⇒ MessagePlusCurrentAggregate[Cmd, Agg] = { msg ⇒
    msg.copy(aggregate = msg.aggregate.flatMap(_.body))
  }

  import ValidationDSL._
  override def commandValidator: AsyncCommandValidator[Cmd, StateMessage[Agg]] = AsyncCommandValidator { msg ⇒
    Seq(
      convertStatePlusMeta(msg) mustSatisfy cmdValidator)
  }

  def cmdValidator: AsyncCommandValidator[Cmd, Agg]
}
