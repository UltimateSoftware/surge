// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.NotUsed
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.AmqpFlow
import akka.stream.scaladsl.{ Flow, Keep }
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import org.slf4j.LoggerFactory
import surge.core.SurgeEventWriteFormatting
import surge.streams.{ EventHandler, EventPlusStreamMeta }

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

trait RabbitEventSink[Event] extends EventHandler[Event] {
  def rabbitMqUri: String
  def queueName: String
  def autoDeclarePlan: Option[AutoDeclarePlan] = None
  def writeRoute: String

  def bufferSize: Int = 10
  def confirmationTimeout: FiniteDuration = 200.millis
  def formatting: SurgeEventWriteFormatting[Event]

  private lazy val connectionProvider: AmqpConnectionProvider = AmqpUriConnectionProvider(rabbitMqUri)

  private val log = LoggerFactory.getLogger(getClass)

  override def eventHandler[Meta]: Flow[EventPlusStreamMeta[String, Event, Meta], Meta, NotUsed] = {
    Flow[EventPlusStreamMeta[String, Event, Meta]]
      .map { evtPlusOffset =>
        val serialized = formatting.writeEvent(evtPlusOffset.messageBody)
        val headers: Map[String, AnyRef] = serialized.headers
        val props = new BasicProperties.Builder().headers(headers.asJava).build()

        val writeMessage = WriteMessage(ByteString(serialized.value)).withProperties(props).withRoutingKey(writeRoute)
        interceptWriteMessage(writeMessage) -> evtPlusOffset.streamMeta

      }
      .via(PassThroughFlow(
        Flow[(WriteMessage, Meta)].map(_._1).via(AmqpFlow.withConfirm(writeSettings())), // TODO Grab the write result and look for failures?
        Keep.right))
      .map(_._2)
  }

  def interceptWriteMessage(writeMessage: WriteMessage): WriteMessage = {
    writeMessage
  }

  protected[rabbit] def logDeclarations(declarations: Seq[Declaration]): Seq[Declaration] = {
    log.debug("RabbitDataSource Declarations")
    declarations.foreach(d => log.debug("declaration => ", d))
    declarations
  }

  protected[rabbit] def joinDeclarations(declarations: Declaration*): scala.collection.immutable.Seq[Declaration] = {
    logDeclarations(declarations.toSeq).toVector
  }

  protected[rabbit] def declarations(plan: AutoDeclarePlan): scala.collection.immutable.Seq[Declaration] =
    joinDeclarations(plan.queuePlan.declaration(), plan.exchangePlan.declaration(), plan.binding.declaration())

  private def writeSettings(): AmqpWriteSettings = {
    autoDeclarePlan match {
      case Some(plan) =>
        AmqpWriteSettings(connectionProvider)
          .withRoutingKey(writeRoute)
          .withDeclarations(declarations(plan))
          .withBufferSize(bufferSize = bufferSize)
          .withConfirmationTimeout(confirmationTimeout)
      case None =>
        AmqpWriteSettings(connectionProvider).withRoutingKey(writeRoute).withBufferSize(bufferSize = bufferSize).withConfirmationTimeout(confirmationTimeout)
    }
  }
}
