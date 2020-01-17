// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.ultimatesoftware.akka.streams.kafka.KafkaConsumer
import com.ultimatesoftware.kafka.streams.core.SurgeEventReadFormatting
import com.ultimatesoftware.scala.core.domain.{ CommandMetadata, ConsumedEventCommandMetadata, DefaultCommandMetadata }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.messaging.EventProperties
import org.slf4j.{ Logger, LoggerFactory }

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

trait EventTransformer[UpstreamEvent, Command] {
  def transform(event: UpstreamEvent): Optional[Command]
}

trait MetadataExtractor[CmdMeta] {
  def extractMetadata(props: EventProperties): CmdMeta
}

class UltiUpstreamEventSourceToSurgeSink[AggId, UpstreamEvent, Command](
    kafkaTopic: KafkaTopic,
    surgeEngine: KafkaStreamsCommand[AggId, _, Command, _, CommandMetadata[_], _],
    readFormatting: SurgeEventReadFormatting[UpstreamEvent, EventProperties],
    eventTransformer: EventTransformer[UpstreamEvent, Command],
    parallelism: Int = 1) {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  private implicit val ec: ExecutionContext = ExecutionContext.global

  private def sendToSurge(key: String, value: Array[Byte]): Future[Done] = {
    val eventOpt = Try(readFormatting.readEvent(value)).toOption
    eventOpt match {
      case Some(eventPlusMetadata) ⇒
        val event = eventPlusMetadata._1
        eventTransformer.transform(event).asScala.map { command ⇒
          val aggregateRef = surgeEngine.aggregateFor(surgeEngine.businessLogic.model.aggIdFromCommand(command))
          val cmdMetaOpt = eventPlusMetadata._2.map(ConsumedEventCommandMetadata.fromEventProperties)
          val cmdMeta = cmdMetaOpt.getOrElse(DefaultCommandMetadata.empty())

          aggregateRef.ask(cmdMeta, command).toScala.map(_ ⇒ Done)
        } getOrElse {
          log.info(s"Skipping event with class ${event.getClass} since it was not converted into a command")
          Future.successful(Done)
        }
      case None ⇒
        log.error(s"Unable to deserialize event from kafka value, key = $key, value = $value")
        Future.successful(Done)
    }

  }

  // FIXME rename this consumer group
  private val consumerGroup = s"surge-upstream-event-consumer-to-${surgeEngine.businessLogic.aggregateName}-aggregate"
  private implicit val actorSystem: ActorSystem = surgeEngine.actorSystem
  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private val settings = KafkaConsumer.consumerSettings(actorSystem, groupId = consumerGroup)

  def create(): UltiUpstreamEventSourceToSurgeSink[AggId, UpstreamEvent, Command] = {
    KafkaConsumer()(surgeEngine.actorSystem).streamAndCommitOffsets(kafkaTopic, sendToSurge, parallelism, settings)
    this
  }
}
