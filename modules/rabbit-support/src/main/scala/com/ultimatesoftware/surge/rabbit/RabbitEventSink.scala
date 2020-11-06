// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.surge.rabbit

import akka.NotUsed
import akka.stream.alpakka.amqp.scaladsl.AmqpFlow
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.ultimatesoftware.kafka.streams.core.{ EventHandler, SurgeEventWriteFormatting }

import scala.collection.JavaConverters._
import scala.concurrent.duration._

trait RabbitEventSink[Event] extends EventHandler[Event] {
  def rabbitMqUri: String
  def queueName: String
  def writeFormatting: SurgeEventWriteFormatting[Event]

  private lazy val connectionProvider: AmqpConnectionProvider = AmqpUriConnectionProvider(rabbitMqUri)
  private lazy val writeSettings = AmqpWriteSettings(connectionProvider)
    .withRoutingKey(queueName)
    .withDeclaration(QueueDeclaration(queueName))
    .withBufferSize(10)
    .withConfirmationTimeout(200.millis)

  private lazy val rabbitWriteFlow = AmqpFlow.withConfirm(writeSettings)

  override def eventHandler: Flow[Event, Any, NotUsed] = {
    Flow[Event].map { event ⇒ writeFormatting.writeEvent(event) }
      .map { serialized ⇒
        val headers: Map[String, AnyRef] = serialized.headers
        val props = new BasicProperties.Builder()
          .headers(headers.asJava)
          .build()

        WriteMessage(ByteString(serialized.value))
          .withProperties(props)
        //.withRoutingKey(serialized.key) // TODO Do we want this?
      }
      .via(rabbitWriteFlow) // TODO Grab the write result and look for failures?
  }
}
