// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.NotUsed
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.AmqpFlow
import akka.stream.scaladsl.{ Flow, Keep }
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import surge.akka.streams.graph.PassThroughFlow
import surge.core.{ EventHandler, EventPlusStreamMeta, SurgeEventWriteFormatting }

import scala.collection.JavaConverters._
import scala.concurrent.duration._

trait RabbitEventSink[Event] extends EventHandler[Event] {
  def rabbitMqUri: String
  def queueName: String
  def durableQueue: Boolean = false
  def autoDeleteQueue: Boolean = false
  def exclusiveQueue: Boolean = false

  def bufferSize: Int = 10
  def confirmationTimeout: FiniteDuration = 200.millis

  /**
   * Queue Arguments used to configure the write settings for `queueName`
   *
   * @see akka.stream.alpakka.amqp.QueueDeclaration
   * @return Map[String, AnyRef]
   */
  def queueArguments: Map[String, AnyRef] = Map.empty
  def writeFormatting: SurgeEventWriteFormatting[Event]

  private lazy val connectionProvider: AmqpConnectionProvider = AmqpUriConnectionProvider(rabbitMqUri)
  private lazy val writeSettings = AmqpWriteSettings(connectionProvider)
    .withRoutingKey(queueName)
    .withDeclaration(QueueDeclaration(queueName)
      .withDurable(durableQueue)
      .withAutoDelete(autoDeleteQueue)
      .withExclusive(exclusiveQueue)
      .withArguments(queueArguments))
    .withBufferSize(bufferSize = bufferSize)
    .withConfirmationTimeout(confirmationTimeout)

  private lazy val rabbitWriteFlow = AmqpFlow.withConfirm(writeSettings)

  override def eventHandler[Meta]: Flow[EventPlusStreamMeta[String, Event, Meta], Meta, NotUsed] = {
    Flow[EventPlusStreamMeta[String, Event, Meta]].map { evtPlusOffset =>
      val serialized = writeFormatting.writeEvent(evtPlusOffset.messageBody)
      val headers: Map[String, AnyRef] = serialized.headers
      val props = new BasicProperties.Builder()
        .headers(headers.asJava)
        .build()

      WriteMessage(ByteString(serialized.value)).withProperties(props) -> evtPlusOffset.streamMeta

    }.via(
      PassThroughFlow(
        Flow[(WriteMessage, Meta)].map(_._1).via(rabbitWriteFlow), // TODO Grab the write result and look for failures?
        Keep.right)).map(_._2)
  }
}
