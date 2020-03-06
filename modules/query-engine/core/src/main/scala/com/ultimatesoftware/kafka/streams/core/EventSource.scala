// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.ultimatesoftware.akka.streams.kafka.KafkaConsumer
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.{ ExecutionContext, Future }

trait EventSource[Event, EvtMeta] {
  def kafkaTopic: KafkaTopic
  def parallelism: Int
  def consumerGroup: String
  def formatting: SurgeEventReadFormatting[Event, EvtMeta]

  private implicit val ec: ExecutionContext = ExecutionContext.global

  private val log: Logger = LoggerFactory.getLogger(getClass)
  private implicit val actorSystem: ActorSystem = ActorSystem()
  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private lazy val settings = KafkaConsumer.consumerSettings(actorSystem, groupId = consumerGroup)
  private lazy val envelopeUtils = new EnvelopeUtils(formatting)

  private def eventHandler(sink: EventSink[Event, EvtMeta])(key: String, value: Array[Byte]): Future[Done] = {
    envelopeUtils.eventFromBytes(value) match {
      case Some(eventPlusMeta) ⇒
        sink.handleEvent(eventPlusMeta.event, eventPlusMeta.meta).map(_ ⇒ Done)
      case None ⇒
        log.error(s"Unable to deserialize event from kafka value, key = $key, value = $value")
        Future.successful(Done)
    }
  }

  def to(sink: EventSink[Event, EvtMeta]): Unit = {
    KafkaConsumer().streamAndCommitOffsets(kafkaTopic, eventHandler(sink), parallelism, settings)
  }
}
