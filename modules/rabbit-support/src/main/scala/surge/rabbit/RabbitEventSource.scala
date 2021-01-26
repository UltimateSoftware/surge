// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.amqp.javadsl.{ AmqpSource, CommittableReadResult }
import akka.stream.alpakka.amqp.{ AmqpConnectionProvider, AmqpUriConnectionProvider, NamedQueueSourceSettings, QueueDeclaration }
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import akka.util.ByteString
import org.slf4j.LoggerFactory
import surge.akka.streams.graph.PassThroughFlow
import surge.core._
import surge.support.Logging

import scala.collection.JavaConverters._

trait RabbitDataSource[Key, Value] extends DataSource {
  def actorSystem: ActorSystem

  def rabbitMqUri: String

  def queueName: String
  def durableQueue: Boolean = false
  def autoDeleteQueue: Boolean = false
  def exclusiveQueue: Boolean = false
  /**
   * Queue Arguments used to configure the read settings for `queueName`
   *
   * @see akka.stream.alpakka.amqp.QueueDeclaration
   * @return Map[String, AnyRef]
   */
  def queueArguments: Map[String, AnyRef] = Map.empty
  def readResultToKey: CommittableReadResult => Key

  def readResultToValue: ByteString => Value

  private val connectionProvider: AmqpConnectionProvider = AmqpUriConnectionProvider(rabbitMqUri)
  private val queueDeclaration = QueueDeclaration(queueName)
    .withDurable(durableQueue)
    .withExclusive(exclusiveQueue)
    .withAutoDelete(autoDeleteQueue)
    .withArguments(queueArguments)

  private val log = LoggerFactory.getLogger(getClass)
  private def businessFlow(sink: DataHandler[Key, Value]): Flow[CommittableReadResult, CommittableReadResult, NotUsed] = {
    val handleEventFlow = Flow[CommittableReadResult].map { crr =>
      val key = readResultToKey(crr)
      val value = readResultToValue(crr.message.bytes)
      val headers = crr.message.properties.getHeaders.asScala.mapValues(_.toString.getBytes()).toMap
      val eventPlusMeta = EventPlusStreamMeta(key, value, streamMeta = None, headers)
      eventPlusMeta
    }.via(sink.dataHandler)

    Flow[CommittableReadResult].via(PassThroughFlow(handleEventFlow, Keep.right))
  }

  def to(sink: DataHandler[Key, Value]): DataPipeline = {
    AmqpSource.committableSource(
      NamedQueueSourceSettings(connectionProvider, queueName)
        .withDeclaration(queueDeclaration), bufferSize = 10)
      .via(businessFlow(sink))
      .mapAsync(1, cm => cm.ack)
      .runWith(Sink.ignore, actorSystem)

    new RabbitDataPipeline()
  }
}

trait RabbitEventSource[Event] extends RabbitDataSource[String, Array[Byte]]
  with EventSourceDeserialization[Event]
  with Logging {

  override def readResultToKey: CommittableReadResult => String = { readResult => readResult.message.envelope.getRoutingKey }
  override def readResultToValue: ByteString => Array[Byte] = { byteString => byteString.toArray }

  def to(sink: EventHandler[Event]): DataPipeline = {
    super.to(dataHandler(sink))
  }
}

