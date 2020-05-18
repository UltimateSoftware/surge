// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional

import com.ultimatesoftware.kafka.streams.core.{ EventSource, SurgeCommandServiceSink, SurgeEventReadFormatting }
import com.ultimatesoftware.scala.core.domain.DefaultCommandMetadata
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.messaging.EventProperties

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ ExecutionContext, Future }

trait EventTransformer[UpstreamEvent, Command] {
  def transform(event: UpstreamEvent): Optional[Command]
}

abstract class UltiSurgeCommandServiceSink[AggId, Command, UpstreamEvent]
  extends SurgeCommandServiceSink[AggId, Command, DefaultCommandMetadata, UpstreamEvent, EventProperties] {

  override implicit def executionContext: ExecutionContext = ExecutionContext.global

  def eventTransformer: EventTransformer[UpstreamEvent, Command]
  def surgeEngine: KafkaStreamsCommand[AggId, _, Command, _, DefaultCommandMetadata, _]

  override def eventToCommand: UpstreamEvent ⇒ Option[Command] = eventTransformer.transform(_).asScala
  override def evtMetaToCmdMeta(evtMeta: EventProperties): DefaultCommandMetadata = DefaultCommandMetadata.fromEventProperties(evtMeta)

  override protected def sendToAggregate(aggId: AggId, cmdMeta: DefaultCommandMetadata, command: Command): Future[Any] = {
    surgeEngine.aggregateFor(aggId).ask(cmdMeta, command).toScala
  }

  override def handleEvent(event: UpstreamEvent, eventProps: EventProperties): Future[Any] = super.handleEvent(event, eventProps)
}

@deprecated("Extend & implement EventSource and UltiSurgeCommandServiceSink separately and wire them together with EventSource.to(EventSink)", "0.4.8")
class UltiUpstreamEventSourceToSurgeSink[AggId, UpstreamEvent, Command](
    kafkaTopic: KafkaTopic,
    surgeEngine: KafkaStreamsCommand[AggId, _, Command, _, DefaultCommandMetadata, _],
    readFormatting: SurgeEventReadFormatting[UpstreamEvent, EventProperties],
    eventTransformer: EventTransformer[UpstreamEvent, Command],
    parallelism: Int = 1) {

  private val topic = kafkaTopic
  private val engine = surgeEngine
  private val transformer = eventTransformer
  private val _parallelism = parallelism

  val consumerGroup: String = s"surge-upstream-event-consumer-to-${surgeEngine.businessLogic.aggregateName}-aggregate"
  private val group = consumerGroup

  private lazy val eventSource: EventSource[UpstreamEvent, EventProperties] = new EventSource[UpstreamEvent, EventProperties] {
    override def kafkaTopic: KafkaTopic = topic
    override def parallelism: Int = _parallelism
    // FIXME rename this consumer group
    override def consumerGroup: String = group
    override def formatting: SurgeEventReadFormatting[UpstreamEvent, EventProperties] = readFormatting
  }

  private lazy val sink = new UltiSurgeCommandServiceSink[AggId, Command, UpstreamEvent] {
    override def surgeEngine: KafkaStreamsCommand[AggId, _, Command, _, DefaultCommandMetadata, _] = engine
    override def eventTransformer: EventTransformer[UpstreamEvent, Command] = (event: UpstreamEvent) ⇒ transformer.transform(event)
  }

  def create(): UltiUpstreamEventSourceToSurgeSink[AggId, UpstreamEvent, Command] = {
    eventSource.to(sink)
    this
  }
}
