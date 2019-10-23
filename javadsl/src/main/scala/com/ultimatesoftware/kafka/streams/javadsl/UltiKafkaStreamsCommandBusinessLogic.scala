// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import com.ultimatesoftware.scala.core.domain.{ StateMessage, StatePlusMetadata }
import com.ultimatesoftware.scala.core.messaging.{ EventMessage, EventProperties }
import com.ultimatesoftware.scala.core.validations.AsyncCommandValidator
import play.api.libs.json.JsValue

trait UltiKafkaStreamsCommandBusinessLogic[AggId, Agg, Cmd, Event, CmdMeta]
  extends KafkaStreamsCommandBusinessLogic[AggId, StatePlusMetadata[Agg], Cmd, EventMessage[Event], CmdMeta, EventProperties] {
  override def eventKeyExtractor(evtMsg: EventMessage[Event]): String = s"${evtMsg.aggregateId}:${evtMsg.sequenceNumber}"

  override def stateKeyExtractor(jsValue: JsValue): String = {
    jsValue.asOpt[StateMessage[JsValue]].map(_.fullIdentifier).getOrElse("")
  }

  override def commandValidator: AsyncCommandValidator[Cmd, StatePlusMetadata[Agg]] = AsyncCommandValidator { _ ⇒ Seq() }

}
