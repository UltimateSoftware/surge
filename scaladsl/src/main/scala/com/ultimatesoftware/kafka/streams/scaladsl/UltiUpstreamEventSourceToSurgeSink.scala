// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.ultimatesoftware.akka.streams.kafka.KafkaConsumer
import com.ultimatesoftware.scala.core.domain.{ CommandMetadata, ConsumedEventCommandMetadata }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import com.ultimatesoftware.scala.core.messaging.{ EventMessageSerializerRegistry, EventProperties }
import com.ultimatesoftware.scala.core.utils.JsonUtils
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.JsValue

import scala.concurrent.{ ExecutionContext, Future }

case class UltiUpstreamEventSourceToSurgeSink[AggId, UpstreamEvent, Command](
    kafkaTopic: KafkaTopic,
    surgeEngine: KafkaStreamsCommand[AggId, _, Command, _, CommandMetadata[_], _],
    registry: EventMessageSerializerRegistry[UpstreamEvent],
    eventTransformer: UpstreamEvent ⇒ Option[Command],
    parallelism: Int = 1)(implicit ec: ExecutionContext) {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  private def sendToSurge(key: String, value: Array[Byte]): Future[Done] = {
    val jsValue = JsonUtils.parseMaybeCompressedBytes[JsValue](value)
    val eventOpt = jsValue.flatMap { jsVal ⇒
      registry.deserializerFromSchema(jsVal)
    }
    eventOpt match {
      case Some(event) ⇒
        eventTransformer(event.body).map { command ⇒
          val aggregateRef = surgeEngine.aggregateFor(surgeEngine.businessLogic.model.aggIdFromCommand(command))
          val cmdMeta = ConsumedEventCommandMetadata.fromEventProperties(event.toProperties)

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
