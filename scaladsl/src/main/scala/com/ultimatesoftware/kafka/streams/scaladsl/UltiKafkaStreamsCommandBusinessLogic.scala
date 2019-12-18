// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import com.ultimatesoftware.scala.core.domain.{ StateMessage, StatePlusMetadata }
import com.ultimatesoftware.scala.core.messaging.{ EventMessage, EventProperties }
import com.ultimatesoftware.scala.core.validations.{ AsyncCommandValidator, MessagePlusCurrentAggregate, ValidationDSL }
import play.api.libs.json.JsValue

trait UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta, Envelope <: com.ultimatesoftware.mp.serialization.envelope.Envelope]
  extends KafkaStreamsCommandBusinessLogic[AggId, StatePlusMetadata[Agg], Cmd, EventMessage[Event], CmdMeta, EventProperties, Envelope] {

  override def eventKeyExtractor: EventMessage[Event] ⇒ String = { evtMsg ⇒ s"${evtMsg.aggregateId}:${evtMsg.sequenceNumber}" }

  override def stateKeyExtractor: JsValue ⇒ String = { jsValue ⇒
    jsValue.asOpt[StateMessage[JsValue]].map(_.fullIdentifier).getOrElse("")
  }

  private def convertStatePlusMeta: MessagePlusCurrentAggregate[Cmd, StatePlusMetadata[Agg]] ⇒ MessagePlusCurrentAggregate[Cmd, Agg] = { msg ⇒
    msg.copy(aggregate = msg.aggregate.flatMap(_.state))
  }

  import ValidationDSL._
  override def commandValidator: AsyncCommandValidator[Cmd, StatePlusMetadata[Agg]] = AsyncCommandValidator { msg ⇒
    Seq(
      convertStatePlusMeta(msg) mustSatisfy cmdValidator)
  }

  def cmdValidator: AsyncCommandValidator[Cmd, Agg]
}
