// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.kafka

import java.time.Instant
import java.util.Properties

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Producer
import akka.kafka.{ ProducerMessage, ProducerSettings }
import akka.stream.scaladsl.Flow
import akka.{ Done, NotUsed }
import com.typesafe.config.{ Config, ConfigFactory }
import com.ultimatesoftware.scala.core.kafka.{ KafkaSecurityConfiguration, KafkaTopic }
import org.apache.kafka.clients.producer.{ ProducerRecord, KafkaProducer => ApacheKafkaProducer }
import org.apache.kafka.common.serialization._

import scala.collection.JavaConverters._

trait KafkaProducerTrait extends KafkaSecurityConfiguration {

  private val config: Config = ConfigFactory.load()

  def producerSettings(
    actorSystem: ActorSystem,
    clientIdOpt: Option[String] = None): ProducerSettings[String, Array[Byte]] = {
    val (keySerializer, valueSerializer) = (new StringSerializer, new ByteArraySerializer)
    val baseSettings = ProducerSettings(actorSystem, keySerializer, valueSerializer)
    val settings = clientIdOpt.map(clientId ⇒ baseSettings.withProperty("client.id", clientId)).getOrElse(baseSettings)

    val securityProps = new Properties()
    configureSecurityProperties(securityProps)

    val brokers = config.getString("kafka.brokers")
    settings
      .withBootstrapServers(brokers)
      .withProperties(securityProps.asScala.toMap)
  }

  def createPassThroughFlow[Message, PassThrough](
    actorSystem: ActorSystem,
    kafkaTopic: KafkaTopic,
    keyOf: Message ⇒ Option[String],
    valueOf: Message ⇒ Array[Byte],
    timestampOf: Message ⇒ Instant,
    passThroughFor: Message ⇒ PassThrough,
    clientIdOpt: Option[String] = None,
    producerOpt: Option[ApacheKafkaProducer[String, Array[Byte]]] = None): Flow[Message, PassThrough, NotUsed] = {

    val settings = producerSettings(actorSystem, clientIdOpt)

    Flow[Message]
      .map { message ⇒
        val producerRecord = new ProducerRecord[String, Array[Byte]](
          kafkaTopic.name, /* Set later via a custom partitioner */ None.orNull,
          timestampOf(message).toEpochMilli, keyOf(message).orNull, valueOf(message))
        // TODO custom partitioning ?
        ProducerMessage.single(producerRecord, passThrough = passThroughFor(message))
      }
      .via {
        producerOpt match {
          case Some(producer) ⇒ Producer.flexiFlow(settings.withProducer(producer))
          case _              ⇒ Producer.flexiFlow(settings)
        }
      }
      .map(result ⇒ result.passThrough)
  }

  def createFlow[Message](
    actorSystem: ActorSystem,
    kafkaTopic: KafkaTopic,
    keyOf: Message ⇒ Option[String],
    valueOf: Message ⇒ Array[Byte],
    timestampOf: Message ⇒ Instant,
    clientIdOpt: Option[String] = None,
    producerOpt: Option[ApacheKafkaProducer[String, Array[Byte]]] = None): Flow[Message, Done, NotUsed] = {
    createPassThroughFlow[Message, NotUsed](actorSystem = actorSystem, kafkaTopic = kafkaTopic,
      keyOf = keyOf, valueOf = valueOf, timestampOf = timestampOf, passThroughFor = _ ⇒ NotUsed,
      clientIdOpt = clientIdOpt, producerOpt = producerOpt).map(_ ⇒ Done)
  }

}

object KafkaProducer extends KafkaProducerTrait
