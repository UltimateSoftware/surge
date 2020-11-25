// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.AmqpFlow
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import org.slf4j.LoggerFactory
import surge.core.{ EventHandler, EventPlusOffset, SurgeEventWriteFormatting }

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

  /*
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
  */
  private val log = LoggerFactory.getLogger(getClass)
  override def eventHandler: Flow[EventPlusOffset[Event], CommittableOffset, NotUsed] = Flow[EventPlusOffset[Event]].map { temp ⇒
    log.error("Rabbit event sink is not yet supported for Surge 0.5.x")
    temp.committableOffset
  }
}
