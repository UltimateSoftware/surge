// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import com.ultimatesoftware.kafka.streams.core.SurgeCommandServiceSink
import com.ultimatesoftware.scala.core.domain.DefaultCommandMetadata
import com.ultimatesoftware.scala.core.messaging.EventProperties

import scala.concurrent.Future

trait UltiSurgeCommandServiceSink[AggId, Command, UpstreamEvent]
  extends SurgeCommandServiceSink[AggId, Command, DefaultCommandMetadata, UpstreamEvent, EventProperties] {

  def surgeEngine: KafkaStreamsCommand[AggId, _, Command, _, DefaultCommandMetadata, _]
  override def evtMetaToCmdMeta(evtMeta: EventProperties): DefaultCommandMetadata = DefaultCommandMetadata.fromEventProperties(evtMeta)
  override protected def sendToAggregate(aggId: AggId, cmdMeta: DefaultCommandMetadata, command: Command): Future[Any] = {
    surgeEngine.aggregateFor(aggId).ask(cmdMeta, command)
  }
}
