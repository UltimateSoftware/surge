// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.ultimatesoftware.akka.streams.kafka.KafkaConsumer
import com.ultimatesoftware.kafka.streams.core.SurgeEventReadFormatting
import com.ultimatesoftware.scala.core.domain.DefaultCommandMetadata
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.messaging.EventProperties
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

case class UltiUpstreamEventSourceToSurgeSink[AggId, UpstreamEvent, Command](
    kafkaTopic: KafkaTopic,
    surgeEngine: KafkaStreamsCommand[AggId, _, Command, _, DefaultCommandMetadata, _],
    readFormatting: SurgeEventReadFormatting[UpstreamEvent, EventProperties],
    eventTransformer: UpstreamEvent ⇒ Option[Command],
    parallelism: Int = 1)(implicit ec: ExecutionContext) {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  private def sendToSurge(key: String, value: Array[Byte]): Future[Done] = {
    val eventOpt = Try(readFormatting.readEvent(value)).toOption
    eventOpt match {
      case Some(eventPlusMetadata) ⇒
        val event = eventPlusMetadata._1
        eventTransformer(event).map { command ⇒
          val aggregateRef = surgeEngine.aggregateFor(surgeEngine.businessLogic.model.aggIdFromCommand(command))
          val cmdMetaOpt = eventPlusMetadata._2.map(DefaultCommandMetadata.fromEventProperties)
          val cmdMeta = cmdMetaOpt.getOrElse(DefaultCommandMetadata.empty())

          aggregateRef.ask(cmdMeta, command).map(_ ⇒ Done)
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

  def create(): Unit = {
    KafkaConsumer()(surgeEngine.actorSystem).streamAndCommitOffsets(kafkaTopic, sendToSurge, parallelism, settings)
  }
}
